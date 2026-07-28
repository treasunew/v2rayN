namespace ServiceLib.Manager;

public sealed class RotatingProxyManager
{
    private const string ConfigFileName = "configRotatingProxy.json";
    private const string DefaultHealthCheckUrl = "https://www.gstatic.com/generate_204";
    private static readonly TimeSpan[] _restartDelays =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(15),
    ];
    private static readonly Lazy<RotatingProxyManager> _instance = new(() => new());

    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly object _snapshotLock = new();
    private readonly object _debounceLock = new();
    private readonly RotatingProxySingboxConfigService _configService = new();

    private Config? _config;
    private Func<bool, string, Task>? _updateFunc;
    private ProcessService? _processService;
    private WindowsJobService? _processJob;
    private CancellationTokenSource? _processMonitorSource;
    private Task? _processMonitorTask;
    private RotatingProxyApiServer? _apiServer;
    private CancellationTokenSource? _apiSource;
    private Task? _apiTask;
    private int _apiPort;
    private RotatingProxyHealthChecker? _healthChecker;
    private CancellationTokenSource? _healthSource;
    private Task? _healthTask;
    private Task? _healthMonitorTask;
    private IDisposable? _subscriptionsUpdatedSubscription;
    private CancellationTokenSource? _debounceSource;
    private Task? _debounceTask;
    private Dictionary<string, RotatingProxyCredential> _credentialMap = [];
    private List<RotatingProxyNodeRuntime> _nodes = [];
    private List<RotatingProxySkippedNode> _skipped = [];
    private long _generation;
    private RotatingProxyServiceStatus _status = new()
    {
        State = ERotatingProxyServiceState.Stopped,
    };
    private RotatingProxySnapshot _snapshot = new()
    {
        Service = new RotatingProxyServiceStatus
        {
            State = ERotatingProxyServiceState.Stopped,
        },
    };
    private RotatingProxyApiSnapshot _apiSnapshot = CreateInitialApiSnapshot();

    public static RotatingProxyManager Instance => _instance.Value;

    public RotatingProxyApiSnapshotProvider SnapshotProvider => GetApiSnapshot;

    public async Task InitializeAsync(Config config, Func<bool, string, Task> updateFunc)
    {
        ArgumentNullException.ThrowIfNull(config);

        await _lifecycleGate.WaitAsync();
        try
        {
            _config = config;
            _updateFunc = updateFunc;
            EnsureSubscriptionListener();
        }
        finally
        {
            _lifecycleGate.Release();
        }

        await ReconcileAsync();
    }

    public async Task ReconcileAsync()
    {
        await _lifecycleGate.WaitAsync();
        try
        {
            var config = _config;
            if (config?.RotatingProxyItem?.Enabled != true)
            {
                _subscriptionsUpdatedSubscription?.Dispose();
                _subscriptionsUpdatedSubscription = null;
                await StopRuntimeAsync();
                await StopApiAsync();
                DeleteRuntimeConfig();
                lock (_snapshotLock)
                {
                    _credentialMap = [];
                    _nodes = [];
                    _skipped = [];
                    _status = new RotatingProxyServiceStatus
                    {
                        State = ERotatingProxyServiceState.Disabled,
                    };
                    PublishSnapshotsLocked(false, "The rotating proxy service is disabled.");
                }
                return;
            }

            EnsureSubscriptionListener();
            var hadRuntime = _processService is not null || _healthChecker is not null;
            var generation = Interlocked.Increment(ref _generation);
            lock (_snapshotLock)
            {
                _status = new RotatingProxyServiceStatus
                {
                    State = hadRuntime
                        ? ERotatingProxyServiceState.Rebuilding
                        : ERotatingProxyServiceState.Starting,
                };
                PublishSnapshotsLocked(false, hadRuntime
                    ? "The rotating proxy service is rebuilding."
                    : "The rotating proxy service is starting.");
            }

            await StopRuntimeAsync();
            DeleteRuntimeConfig();

            try
            {
                await EnsureApiAsync(config.RotatingProxyItem.ApiPort);
            }
            catch (Exception)
            {
                SetFaulted("The rotating proxy API port is unavailable.");
                await SafeUpdateAsync(false,
                    $"Rotating proxy API port {config.RotatingProxyItem.ApiPort} is unavailable.");
                return;
            }

            var candidates = await CollectCandidatesAsync(config.RotatingProxyItem.SubscriptionIds);
            var preservedCredentials = GetCredentialMapCopy();
            var result = _configService.Generate(
                config,
                candidates.Nodes,
                candidates.SubscriptionRemarks,
                preservedCredentials);
            result.Skipped.InsertRange(0, candidates.Skipped);

            lock (_snapshotLock)
            {
                _nodes = result.Nodes.Select(RotatingProxyHealthChecker.CloneNode).ToList();
                _skipped = result.Skipped.Select(CloneSkippedNode).ToList();
                _credentialMap = result.CredentialMap
                    .Where(item => _nodes.Any(node => node.IndexId == item.Key))
                    .ToDictionary(item => item.Key, item => CloneCredential(item.Value));
                PublishSnapshotsLocked(false, "The rotating proxy core is not ready.");
            }

            if (result.Nodes.Count == 0)
            {
                lock (_snapshotLock)
                {
                    _status = new RotatingProxyServiceStatus
                    {
                        State = ERotatingProxyServiceState.Degraded,
                        LastError = "No valid rotating proxy nodes are available.",
                    };
                    PublishSnapshotsLocked(false, "No valid rotating proxy nodes are available.");
                }
                return;
            }
            if (!result.Success || result.ConfigJson.IsNullOrEmpty())
            {
                SetFaulted("The rotating proxy configuration could not be generated.");
                return;
            }

            var configPath = Utils.GetBinConfigPath(ConfigFileName);
            try
            {
                await WriteRuntimeConfigAsync(configPath, result.ConfigJson);
                EnsurePortAvailable(config.RotatingProxyItem.MixedPort, "mixed");

                var coreInfo = CoreInfoManager.Instance.GetCoreInfo(ECoreType.sing_box)
                               ?? throw new InvalidOperationException("The sing-box core definition is unavailable.");
                var coreFile = CoreInfoManager.Instance.GetCoreExecFile(coreInfo, out _);
                if (coreFile.IsNullOrEmpty())
                {
                    throw new FileNotFoundException("The sing-box executable is unavailable.");
                }

                using var checkTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var checkResult = await CheckCoreConfigAsync(coreFile, configPath, checkTimeout.Token);
                if (checkResult.ExitCode != 0)
                {
                    throw new InvalidDataException(
                        $"The sing-box configuration check failed with exit code {checkResult.ExitCode}.");
                }

                var processMonitorSource = new CancellationTokenSource();
                var process = await StartCoreProcessAsync(
                    coreFile,
                    coreInfo,
                    configPath,
                    config.RotatingProxyItem.MixedPort,
                    processMonitorSource.Token);
                _processService = process.Process;
                _processJob = process.Job;
                _processMonitorSource = processMonitorSource;
                StartHealthChecker(config, generation);

                lock (_snapshotLock)
                {
                    _status = new RotatingProxyServiceStatus
                    {
                        State = ERotatingProxyServiceState.Running,
                        ProcessId = process.Process.Id,
                        StartedTime = DateTime.UtcNow,
                    };
                    PublishSnapshotsLocked(true);
                }

                _processMonitorTask = MonitorProcessAsync(
                    process.Process,
                    coreFile,
                    coreInfo,
                    configPath,
                    generation,
                    processMonitorSource.Token);
            }
            catch (Exception ex)
            {
                await StopRuntimeAsync();
                DeleteRuntimeConfig();
                var message = GetSafeStartFailureMessage(ex);
                SetFaulted(message);
                await SafeUpdateAsync(false, message);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task StopAsync()
    {
        await CancelDebounceAsync();
        await _lifecycleGate.WaitAsync();
        try
        {
            _subscriptionsUpdatedSubscription?.Dispose();
            _subscriptionsUpdatedSubscription = null;

            lock (_snapshotLock)
            {
                _status.State = ERotatingProxyServiceState.Stopping;
                PublishSnapshotsLocked(false, "The rotating proxy service is stopping.");
            }

            await StopRuntimeAsync();
            await StopApiAsync();
            DeleteRuntimeConfig();

            lock (_snapshotLock)
            {
                _credentialMap = [];
                _nodes = [];
                _skipped = [];
                _status = new RotatingProxyServiceStatus
                {
                    State = ERotatingProxyServiceState.Stopped,
                };
                PublishSnapshotsLocked(false, "The rotating proxy service is stopped.");
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public RotatingProxyServiceStatus GetStatus()
    {
        lock (_snapshotLock)
        {
            return CloneStatus(_status);
        }
    }

    public RotatingProxySnapshot GetSnapshot()
    {
        lock (_snapshotLock)
        {
            return CloneSnapshot(_snapshot);
        }
    }

    public static bool HasSelectedSubscriptionIntersection(
        IEnumerable<string>? selectedSubscriptionIds,
        IReadOnlyList<string>? updatedSubscriptionIds)
    {
        if (updatedSubscriptionIds is not { Count: > 0 })
        {
            return false;
        }

        var selected = new HashSet<string>(
            (selectedSubscriptionIds ?? []).Where(id => id.IsNotEmpty()),
            StringComparer.Ordinal);
        return selected.Count > 0 && updatedSubscriptionIds.Any(selected.Contains);
    }

    private RotatingProxyApiSnapshot GetApiSnapshot()
    {
        return Volatile.Read(ref _apiSnapshot);
    }

    private void EnsureSubscriptionListener()
    {
        _subscriptionsUpdatedSubscription ??= AppEvents.SubscriptionsUpdated
            .AsObservable()
            .Subscribe(OnSubscriptionsUpdated);
    }

    private void OnSubscriptionsUpdated(IReadOnlyList<string> updatedIds)
    {
        var selectedIds = _config?.RotatingProxyItem?.SubscriptionIds;
        if (!HasSelectedSubscriptionIntersection(selectedIds, updatedIds))
        {
            return;
        }

        lock (_debounceLock)
        {
            _debounceSource?.Cancel();
            _debounceSource?.Dispose();
            _debounceSource = new CancellationTokenSource();
            _debounceTask = DebouncedReconcileAsync(_debounceSource.Token);
        }
    }

    private async Task DebouncedReconcileAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            await ReconcileAsync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer subscription update replaced this reconciliation request.
        }
        catch (Exception)
        {
            SetFaulted("The rotating proxy subscription reconciliation failed.");
        }
    }

    private async Task CancelDebounceAsync()
    {
        CancellationTokenSource? source;
        Task? task;
        lock (_debounceLock)
        {
            source = _debounceSource;
            task = _debounceTask;
            _debounceSource = null;
            _debounceTask = null;
        }

        source?.Cancel();
        if (task is not null)
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
                // Expected when the debounce is canceled.
            }
        }
        source?.Dispose();
    }

    private async Task<CandidateCollection> CollectCandidatesAsync(IReadOnlyList<string> selectedIds)
    {
        var subscriptions = await AppManager.Instance.SubItems() ?? [];
        var subscriptionMap = subscriptions
            .Where(item => item.Id.IsNotEmpty())
            .GroupBy(item => item.Id)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);
        var nodes = new List<ProfileItem>();
        var skipped = new List<RotatingProxySkippedNode>();
        var remarks = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var id in selectedIds.Where(id => id.IsNotEmpty()).Distinct(StringComparer.Ordinal))
        {
            if (!subscriptionMap.TryGetValue(id, out var subscription))
            {
                skipped.Add(new RotatingProxySkippedNode
                {
                    SubscriptionId = id,
                    Reason = ERotatingProxySkipReason.SubscriptionMissing,
                });
                continue;
            }
            if (!subscription.Enabled)
            {
                skipped.Add(new RotatingProxySkippedNode
                {
                    SubscriptionId = id,
                    SubscriptionRemarks = subscription.Remarks ?? string.Empty,
                    Reason = ERotatingProxySkipReason.SubscriptionDisabled,
                });
                continue;
            }

            remarks[id] = subscription.Remarks ?? string.Empty;
            var profiles = await AppManager.Instance.ProfileItems(id);
            if (profiles is { Count: > 0 })
            {
                nodes.AddRange(profiles);
            }
        }

        return new CandidateCollection(nodes, skipped, remarks);
    }

    private async Task EnsureApiAsync(int apiPort)
    {
        if (_apiServer is not null
            && _apiPort == apiPort
            && _apiTask is { IsCompleted: false })
        {
            return;
        }

        await StopApiAsync();
        var source = new CancellationTokenSource();
        var server = new RotatingProxyApiServer(apiPort, SnapshotProvider);
        var task = server.RunAsync(source.Token);
        try
        {
            using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await server.WaitUntilStartedAsync(timeoutSource.Token);
        }
        catch
        {
            source.Cancel();
            try
            {
                await task;
            }
            catch
            {
                // The startup exception is propagated below.
            }
            await server.DisposeAsync();
            source.Dispose();
            throw;
        }

        _apiSource = source;
        _apiServer = server;
        _apiTask = task;
        _apiPort = apiPort;
    }

    private async Task StopApiAsync()
    {
        var source = _apiSource;
        var server = _apiServer;
        var task = _apiTask;
        _apiSource = null;
        _apiServer = null;
        _apiTask = null;
        _apiPort = 0;

        source?.Cancel();
        if (server is not null)
        {
            try
            {
                await server.DisposeAsync();
            }
            catch
            {
                // Best-effort API shutdown.
            }
        }
        if (task is not null)
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
            catch
            {
                // The service state already reports API startup/runtime failures.
            }
        }
        source?.Dispose();
    }

    private void StartHealthChecker(Config config, long generation)
    {
        var item = config.RotatingProxyItem;
        var healthUrl = config.SpeedTestItem?.SpeedPingTestUrl;
        if (!Uri.TryCreate(healthUrl, UriKind.Absolute, out var healthUri)
            || (healthUri.Scheme != Uri.UriSchemeHttp && healthUri.Scheme != Uri.UriSchemeHttps))
        {
            healthUrl = DefaultHealthCheckUrl;
        }

        List<RotatingProxyNodeRuntime> nodes;
        Dictionary<string, RotatingProxyCredential> credentials;
        lock (_snapshotLock)
        {
            nodes = _nodes.Select(RotatingProxyHealthChecker.CloneNode).ToList();
            credentials = _credentialMap.ToDictionary(
                item => item.Key,
                item => CloneCredential(item.Value));
        }

        var source = new CancellationTokenSource();
        var checker = new RotatingProxyHealthChecker(
            nodes,
            credentials,
            item.MixedPort,
            healthUrl!,
            TimeSpan.FromSeconds(item.HealthCheckTimeoutSeconds),
            item.HealthCheckConcurrency,
            TimeSpan.FromSeconds(item.HealthyMaxAgeSeconds),
            TimeSpan.FromSeconds(item.HealthCheckIntervalSeconds));
        _healthSource = source;
        _healthChecker = checker;
        _healthTask = checker.RunAsync(source.Token);
        _healthMonitorTask = MonitorHealthAsync(checker, generation, source.Token);
    }

    private async Task MonitorHealthAsync(
        RotatingProxyHealthChecker checker,
        long generation,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                var nodes = checker.GetSnapshot();
                lock (_snapshotLock)
                {
                    if (generation != _generation || !ReferenceEquals(checker, _healthChecker))
                    {
                        return;
                    }

                    _nodes = nodes.Select(RotatingProxyHealthChecker.CloneNode).ToList();
                    var anyChecked = _nodes.Any(node => node.LastHealthCheckTime is not null);
                    var anyHealthy = checker.GetHealthyNodes().Count > 0;
                    if (_status.State is ERotatingProxyServiceState.Running or ERotatingProxyServiceState.Degraded)
                    {
                        _status.State = anyChecked && !anyHealthy
                            ? ERotatingProxyServiceState.Degraded
                            : ERotatingProxyServiceState.Running;
                    }
                    PublishSnapshotsLocked(_processService is { HasExited: false });
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal health monitor shutdown.
        }
    }

    private async Task StopHealthAsync()
    {
        var source = _healthSource;
        var task = _healthTask;
        var monitorTask = _healthMonitorTask;
        _healthSource = null;
        _healthChecker = null;
        _healthTask = null;
        _healthMonitorTask = null;

        source?.Cancel();
        foreach (var backgroundTask in new[] { task, monitorTask })
        {
            if (backgroundTask is null)
            {
                continue;
            }
            try
            {
                await backgroundTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
            catch
            {
                // Health failures are represented in the immutable snapshot.
            }
        }
        source?.Dispose();
    }

    private async Task StopRuntimeAsync()
    {
        var monitorSource = _processMonitorSource;
        var monitorTask = _processMonitorTask;
        var process = _processService;
        var job = _processJob;
        _processMonitorSource = null;
        _processMonitorTask = null;
        _processService = null;
        _processJob = null;

        monitorSource?.Cancel();
        await StopHealthAsync();
        if (process is not null)
        {
            try
            {
                await process.StopAsync();
            }
            catch
            {
                // Best-effort process shutdown.
            }
            process.Dispose();
        }
        job?.Dispose();
        if (monitorTask is not null)
        {
            try
            {
                await monitorTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
            catch
            {
                // The monitor publishes a faulted snapshot before it exits.
            }
        }
        monitorSource?.Dispose();
    }

    private async Task MonitorProcessAsync(
        ProcessService currentProcess,
        string coreFile,
        CoreInfo coreInfo,
        string configPath,
        long generation,
        CancellationToken cancellationToken)
    {
        var restartAttempt = 0;
        while (true)
        {
            try
            {
                await currentProcess.WaitForExitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await _lifecycleGate.WaitAsync(cancellationToken);
            try
            {
                if (generation != _generation || !ReferenceEquals(currentProcess, _processService))
                {
                    return;
                }

                await StopHealthAsync();
                var exitCode = currentProcess.ExitCode;
                currentProcess.Dispose();
                _processService = null;
                _processJob?.Dispose();
                _processJob = null;
                ClearHealthPool();
                SetFaulted($"The rotating proxy core exited unexpectedly with code {exitCode?.ToString() ?? "unknown"}.");
            }
            finally
            {
                _lifecycleGate.Release();
            }

            ProcessStartResult? restarted = null;
            while (restartAttempt < _restartDelays.Length && restarted is null)
            {
                var delay = _restartDelays[restartAttempt++];
                await Task.Delay(delay, cancellationToken);
                await _lifecycleGate.WaitAsync(cancellationToken);
                try
                {
                    if (generation != _generation
                        || _config?.RotatingProxyItem?.Enabled != true
                        || cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    lock (_snapshotLock)
                    {
                        _status.State = ERotatingProxyServiceState.Starting;
                        _status.LastError = null;
                        PublishSnapshotsLocked(false, "The rotating proxy core is restarting.");
                    }

                    try
                    {
                        EnsurePortAvailable(_config.RotatingProxyItem.MixedPort, "mixed");
                        restarted = await StartCoreProcessAsync(
                            coreFile,
                            coreInfo,
                            configPath,
                            _config.RotatingProxyItem.MixedPort,
                            cancellationToken);
                        _processService = restarted.Process;
                        _processJob = restarted.Job;
                        StartHealthChecker(_config, generation);
                        lock (_snapshotLock)
                        {
                            _status = new RotatingProxyServiceStatus
                            {
                                State = ERotatingProxyServiceState.Running,
                                ProcessId = restarted.Process.Id,
                                StartedTime = DateTime.UtcNow,
                            };
                            PublishSnapshotsLocked(true);
                        }
                    }
                    catch (Exception)
                    {
                        SetFaulted("The rotating proxy core restart failed.");
                    }
                }
                finally
                {
                    _lifecycleGate.Release();
                }
            }

            if (restarted is null)
            {
                return;
            }
            currentProcess = restarted.Process;
        }
    }

    private async Task<ProcessStartResult> StartCoreProcessAsync(
        string coreFile,
        CoreInfo coreInfo,
        string configPath,
        int mixedPort,
        CancellationToken cancellationToken)
    {
        var environmentVars = new Dictionary<string, string>();
        foreach (var item in coreInfo.Environment)
        {
            environmentVars[item.Key] = string.Format(
                item.Value ?? string.Empty,
                configPath.AppendQuotes());
        }

        ProcessService? process = null;
        WindowsJobService? job = null;
        try
        {
            process = new ProcessService(
                fileName: coreFile,
                arguments: string.Format(coreInfo.Arguments ?? string.Empty, configPath.AppendQuotes()),
                workingDirectory: Utils.GetBinConfigPath(),
                displayLog: true,
                redirectInput: false,
                environmentVars: environmentVars,
                updateFunc: null);
            await process.StartAsync();

            if (Utils.IsWindows())
            {
                job = new WindowsJobService();
                if (!job.AddProcess(process.Handle))
                {
                    throw new InvalidOperationException("The rotating proxy process could not be assigned to its Windows job.");
                }
            }

            await Task.Delay(100, cancellationToken);
            if (process.HasExited)
            {
                throw new InvalidOperationException(
                    $"The rotating proxy core exited during startup with code {process.ExitCode?.ToString() ?? "unknown"}.");
            }

            await WaitForTcpPortAsync(mixedPort, process, TimeSpan.FromSeconds(5), cancellationToken);
            return new ProcessStartResult(process, job);
        }
        catch
        {
            if (process is not null)
            {
                try
                {
                    await process.StopAsync();
                }
                catch
                {
                    // Best-effort failed-start cleanup.
                }
                process.Dispose();
            }
            job?.Dispose();
            throw;
        }
    }

    private static async Task WaitForTcpPortAsync(
        int port,
        ProcessService process,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = new CancellationTokenSource(timeout);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutSource.Token);
        while (!linkedSource.IsCancellationRequested)
        {
            if (process.HasExited)
            {
                throw new InvalidOperationException(
                    $"The rotating proxy core exited before port {port} became ready.");
            }

            using var client = new TcpClient();
            using var attemptSource = CancellationTokenSource.CreateLinkedTokenSource(linkedSource.Token);
            attemptSource.CancelAfter(TimeSpan.FromMilliseconds(250));
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, port, attemptSource.Token);
                return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Retry until the overall startup timeout expires.
            }
            catch (SocketException)
            {
                // The process has not bound the port yet.
            }

            await Task.Delay(50, linkedSource.Token);
        }

        throw new TimeoutException($"The rotating proxy mixed port {port} did not become ready.");
    }

    private static string GetSafeStartFailureMessage(Exception ex)
    {
        return ex switch
        {
            FileNotFoundException => "The sing-box executable is unavailable.",
            InvalidDataException => "The rotating proxy configuration is invalid.",
            TimeoutException => "The rotating proxy core did not become ready in time.",
            OperationCanceledException => "The rotating proxy core startup was canceled.",
            InvalidOperationException { InnerException: SocketException } =>
                ex.Message.IsNotEmpty()
                    ? ex.Message
                    : "The rotating proxy mixed port is already in use.",
            InvalidOperationException when ex.Message.Contains("port", StringComparison.OrdinalIgnoreCase)
                => ex.Message,
            _ => "The rotating proxy core could not be started.",
        };
    }

    private static void EnsurePortAvailable(int port, string name)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        try
        {
            listener.Start();
        }
        catch (SocketException ex)
        {
            throw new InvalidOperationException(
                $"The rotating proxy {name} port {port} is already in use.", ex);
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task<CoreCheckResult> CheckCoreConfigAsync(
        string coreFile,
        string configPath,
        CancellationToken cancellationToken)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = coreFile,
                WorkingDirectory = Utils.GetBinConfigPath(),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8,
            },
        };
        process.StartInfo.ArgumentList.Add("check");
        process.StartInfo.ArgumentList.Add("-c");
        process.StartInfo.ArgumentList.Add(configPath);

        process.Start();
        var standardOutputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var standardErrorTask = process.StandardError.ReadToEndAsync(cancellationToken);
        try
        {
            await process.WaitForExitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            try
            {
                process.Kill(true);
            }
            catch
            {
                // Best-effort canceled check cleanup.
            }
            throw;
        }

        return new CoreCheckResult(
            process.ExitCode,
            await standardOutputTask,
            await standardErrorTask);
    }

    private static async Task WriteRuntimeConfigAsync(string configPath, string configJson)
    {
        var bytes = new UTF8Encoding(false).GetBytes(configJson);
        await using (var stream = new FileStream(
                         configPath,
                         FileMode.Create,
                         FileAccess.Write,
                         FileShare.None,
                         4096,
                         FileOptions.Asynchronous | FileOptions.WriteThrough))
        {
            await stream.WriteAsync(bytes);
            await stream.FlushAsync();
        }

        if (Utils.IsNonWindows())
        {
            File.SetUnixFileMode(
                configPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
    }

    private static void DeleteRuntimeConfig()
    {
        try
        {
            var path = Utils.GetBinConfigPath(ConfigFileName);
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Best-effort removal of the credential-bearing runtime config.
        }
    }

    private void ClearHealthPool()
    {
        lock (_snapshotLock)
        {
            foreach (var node in _nodes)
            {
                node.State = ERotatingProxyNodeState.Unknown;
                node.Delay = null;
                node.LastHealthCheckTime = null;
                node.LastHealthyTime = null;
                node.LastErrorType = null;
                node.LastError = null;
            }
            PublishSnapshotsLocked(false, "The rotating proxy core exited unexpectedly.");
        }
    }

    private void SetFaulted(string message)
    {
        lock (_snapshotLock)
        {
            _status = new RotatingProxyServiceStatus
            {
                State = ERotatingProxyServiceState.Faulted,
                LastError = message,
            };
            PublishSnapshotsLocked(false, message);
        }
    }

    private async Task SafeUpdateAsync(bool notify, string message)
    {
        try
        {
            if (_updateFunc is not null)
            {
                await _updateFunc(notify, message);
            }
        }
        catch
        {
            // UI reporting must not break service lifecycle cleanup.
        }
    }

    private Dictionary<string, RotatingProxyCredential> GetCredentialMapCopy()
    {
        lock (_snapshotLock)
        {
            return _credentialMap.ToDictionary(
                item => item.Key,
                item => CloneCredential(item.Value));
        }
    }

    private void PublishSnapshotsLocked(bool isReady, string? notReadyReason = null)
    {
        var item = _config?.RotatingProxyItem;
        var status = CloneStatus(_status);
        var nodes = _nodes.Select(RotatingProxyHealthChecker.CloneNode).ToArray();
        var skipped = _skipped.Select(CloneSkippedNode).ToList();
        var credentials = _credentialMap.ToDictionary(
            entry => entry.Key,
            entry => CloneCredential(entry.Value));
        var snapshot = new RotatingProxySnapshot
        {
            CreatedTime = DateTime.UtcNow,
            IsReady = isReady,
            Generation = _generation,
            Service = status,
            Nodes = nodes.Select(RotatingProxyHealthChecker.CloneNode).ToList(),
            Skipped = skipped,
        };
        var apiSnapshot = new RotatingProxyApiSnapshot(
            isReady,
            _generation,
            item?.MixedPort ?? 0,
            item?.HealthyMaxAgeSeconds ?? 0,
            status,
            nodes,
            credentials,
            notReadyReason);
        _snapshot = snapshot;
        Volatile.Write(ref _apiSnapshot, apiSnapshot);
    }

    private static RotatingProxySnapshot CloneSnapshot(RotatingProxySnapshot snapshot)
    {
        return new RotatingProxySnapshot
        {
            CreatedTime = snapshot.CreatedTime,
            IsReady = snapshot.IsReady,
            Generation = snapshot.Generation,
            Service = CloneStatus(snapshot.Service),
            Nodes = snapshot.Nodes.Select(RotatingProxyHealthChecker.CloneNode).ToList(),
            Skipped = snapshot.Skipped.Select(CloneSkippedNode).ToList(),
        };
    }

    private static RotatingProxyServiceStatus CloneStatus(RotatingProxyServiceStatus status)
    {
        return new RotatingProxyServiceStatus
        {
            State = status.State,
            ProcessId = status.ProcessId,
            StartedTime = status.StartedTime,
            LastError = status.LastError,
        };
    }

    private static RotatingProxySkippedNode CloneSkippedNode(RotatingProxySkippedNode node)
    {
        return new RotatingProxySkippedNode
        {
            IndexId = node.IndexId,
            SubscriptionId = node.SubscriptionId,
            Remarks = node.Remarks,
            SubscriptionRemarks = node.SubscriptionRemarks,
            Reason = node.Reason,
        };
    }

    private static RotatingProxyCredential CloneCredential(RotatingProxyCredential credential)
    {
        return new RotatingProxyCredential
        {
            Username = credential.Username,
            Password = credential.Password,
        };
    }

    private static RotatingProxyApiSnapshot CreateInitialApiSnapshot()
    {
        return new RotatingProxyApiSnapshot(
            false,
            0,
            0,
            0,
            new RotatingProxyServiceStatus
            {
                State = ERotatingProxyServiceState.Stopped,
            },
            [],
            new Dictionary<string, RotatingProxyCredential>(),
            "The rotating proxy service has not been initialized.");
    }

    private sealed record CandidateCollection(
        List<ProfileItem> Nodes,
        List<RotatingProxySkippedNode> Skipped,
        Dictionary<string, string> SubscriptionRemarks);

    private sealed record ProcessStartResult(ProcessService Process, WindowsJobService? Job);

    private sealed record CoreCheckResult(int ExitCode, string StandardOutput, string StandardError);
}
