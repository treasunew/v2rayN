namespace ServiceLib.Services.RotatingProxy;

public sealed class RotatingProxyHealthChecker
{
    private readonly IReadOnlyList<RotatingProxyNodeRuntime> _nodes;
    private readonly IReadOnlyDictionary<string, int> _nodeIndexes;
    private readonly IReadOnlyDictionary<string, RotatingProxyCredential> _credentialMap;
    private readonly int _mixedPort;
    private readonly Uri _healthCheckUri;
    private readonly TimeSpan _timeout;
    private readonly int _concurrency;
    private readonly TimeSpan _healthyMaxAge;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _roundGate = new(1, 1);
    private readonly object _snapshotLock = new();
    private RotatingProxyNodeRuntime[] _snapshot;

    public RotatingProxyHealthChecker(
        IEnumerable<RotatingProxyNodeRuntime> nodes,
        IReadOnlyDictionary<string, RotatingProxyCredential> credentialMap,
        int mixedPort,
        string healthCheckUrl,
        TimeSpan timeout,
        int concurrency,
        TimeSpan healthyMaxAge,
        TimeSpan interval,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(credentialMap);
        ArgumentException.ThrowIfNullOrWhiteSpace(healthCheckUrl);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(concurrency);
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        if (healthyMaxAge <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(healthyMaxAge));
        }
        if (interval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(interval));
        }
        if (mixedPort is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(mixedPort));
        }
        if (!Uri.TryCreate(healthCheckUrl, UriKind.Absolute, out var healthCheckUri)
            || (healthCheckUri.Scheme != Uri.UriSchemeHttp && healthCheckUri.Scheme != Uri.UriSchemeHttps))
        {
            throw new ArgumentException("The health check URL must be an absolute HTTP or HTTPS URL.",
                nameof(healthCheckUrl));
        }

        _nodes = nodes.Select(CloneNode).ToArray();
        _credentialMap = credentialMap.ToDictionary(
            item => item.Key,
            item => new RotatingProxyCredential
            {
                Username = item.Value.Username,
                Password = item.Value.Password,
            });
        _mixedPort = mixedPort;
        _healthCheckUri = healthCheckUri;
        _timeout = timeout;
        _concurrency = concurrency;
        _healthyMaxAge = healthyMaxAge;
        _interval = interval;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _snapshot = _nodes.Select(CloneNode).ToArray();
        _nodeIndexes = _snapshot
            .Select((node, index) => new { node.IndexId, Index = index })
            .ToDictionary(item => item.IndexId, item => item.Index);
    }

    public IReadOnlyList<RotatingProxyNodeRuntime> GetSnapshot()
    {
        lock (_snapshotLock)
        {
            return _snapshot.Select(CloneNode).ToArray();
        }
    }

    public IReadOnlyList<RotatingProxyNodeRuntime> GetHealthyNodes()
    {
        var oldestHealthyTime = _timeProvider.GetUtcNow().UtcDateTime - _healthyMaxAge;
        lock (_snapshotLock)
        {
            return _snapshot
                .Where(node => node.State == ERotatingProxyNodeState.Healthy
                               && node.LastHealthyTime >= oldestHealthyTime)
                .Select(CloneNode)
                .ToArray();
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            await CheckNowAsync(cancellationToken);
            using var timer = new PeriodicTimer(_interval, _timeProvider);
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await CheckNowAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal service shutdown.
        }
    }

    public async Task CheckNowAsync(CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken);
        try
        {
            var roundNodes = GetSnapshot();
            await Parallel.ForEachAsync(roundNodes,
                new ParallelOptions
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = _concurrency,
                },
                async (node, token) =>
                {
                    var result = await CheckNodeAsync(node, token);
                    UpdateSnapshot(result);
                });
        }
        finally
        {
            _roundGate.Release();
        }
    }

    private async ValueTask<RotatingProxyNodeRuntime> CheckNodeAsync(
        RotatingProxyNodeRuntime node,
        CancellationToken cancellationToken)
    {
        if (!_credentialMap.TryGetValue(node.IndexId, out var credential)
            || credential.Username.IsNullOrEmpty()
            || credential.Password.IsNullOrEmpty())
        {
            return CreateFailedNode(node, null, ERotatingProxyHealthErrorType.MissingCredential,
                "No rotating proxy credential is available for this node.");
        }

        var startTimestamp = _timeProvider.GetTimestamp();
        try
        {
            using var handler = new HttpClientHandler
            {
                AllowAutoRedirect = false,
                Proxy = new WebProxy($"http://{Global.Loopback}:{_mixedPort}")
                {
                    Credentials = new NetworkCredential(credential.Username, credential.Password),
                },
                UseCookies = false,
                UseProxy = true,
            };
            using var client = new HttpClient(handler)
            {
                Timeout = Timeout.InfiniteTimeSpan,
            };
            using var request = new HttpRequestMessage(HttpMethod.Get, _healthCheckUri);
            using var timeoutSource = new CancellationTokenSource(_timeout, _timeProvider);
            using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeoutSource.Token);
            using var response = await client.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, linkedSource.Token);
            var delay = GetElapsedMilliseconds(startTimestamp);
            var checkedTime = _timeProvider.GetUtcNow().UtcDateTime;
            if (response.IsSuccessStatusCode)
            {
                var healthyNode = CloneNode(node);
                healthyNode.State = ERotatingProxyNodeState.Healthy;
                healthyNode.Delay = delay;
                healthyNode.LastHealthCheckTime = checkedTime;
                healthyNode.LastHealthyTime = checkedTime;
                healthyNode.LastErrorType = null;
                healthyNode.LastError = null;
                return healthyNode;
            }

            return CreateFailedNode(node, delay, ERotatingProxyHealthErrorType.HttpStatus,
                $"HTTP {(int)response.StatusCode}.", checkedTime);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return CreateFailedNode(node, GetElapsedMilliseconds(startTimestamp),
                ERotatingProxyHealthErrorType.Timeout,
                $"Health check timed out after {_timeout.TotalSeconds:0.###} seconds.");
        }
        catch (HttpRequestException)
        {
            return CreateFailedNode(node, GetElapsedMilliseconds(startTimestamp),
                ERotatingProxyHealthErrorType.HttpRequest, "Health check request failed.");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return CreateFailedNode(node, GetElapsedMilliseconds(startTimestamp),
                ERotatingProxyHealthErrorType.Unexpected, "Unexpected health check failure.");
        }
    }

    private RotatingProxyNodeRuntime CreateFailedNode(
        RotatingProxyNodeRuntime node,
        int? delay,
        ERotatingProxyHealthErrorType errorType,
        string error,
        DateTime? checkedTime = null)
    {
        var failedNode = CloneNode(node);
        failedNode.State = ERotatingProxyNodeState.Unhealthy;
        failedNode.Delay = delay;
        failedNode.LastHealthCheckTime = checkedTime ?? _timeProvider.GetUtcNow().UtcDateTime;
        failedNode.LastErrorType = errorType;
        failedNode.LastError = error;
        return failedNode;
    }

    private void UpdateSnapshot(RotatingProxyNodeRuntime updatedNode)
    {
        lock (_snapshotLock)
        {
            if (_nodeIndexes.TryGetValue(updatedNode.IndexId, out var index))
            {
                _snapshot[index] = CloneNode(updatedNode);
            }
        }
    }

    private int GetElapsedMilliseconds(long startTimestamp)
    {
        var milliseconds = _timeProvider.GetElapsedTime(startTimestamp).TotalMilliseconds;
        return (int)Math.Clamp(Math.Round(milliseconds), 0, int.MaxValue);
    }

    internal static RotatingProxyNodeRuntime CloneNode(RotatingProxyNodeRuntime node)
    {
        return new RotatingProxyNodeRuntime
        {
            IndexId = node.IndexId,
            SubscriptionId = node.SubscriptionId,
            Remarks = node.Remarks,
            SubscriptionRemarks = node.SubscriptionRemarks,
            OutboundTag = node.OutboundTag,
            Username = node.Username,
            ConfigType = node.ConfigType,
            Address = node.Address,
            Port = node.Port,
            State = node.State,
            Delay = node.Delay,
            LastHealthCheckTime = node.LastHealthCheckTime,
            LastHealthyTime = node.LastHealthyTime,
            LastErrorType = node.LastErrorType,
            LastError = node.LastError,
        };
    }
}
