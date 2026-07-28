namespace ServiceLib.Services.RotatingProxy;

/// <summary>
/// Returns the latest immutable snapshot without blocking.
/// </summary>
public delegate RotatingProxyApiSnapshot RotatingProxyApiSnapshotProvider();

public sealed class RotatingProxyApiServer : IAsyncDisposable
{
    private const int MaxHeaderBytes = 16 * 1024;
    private const int MaxConcurrentConnections = 64;
    private static readonly string[] _protocols = ["http", "socks5"];
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private readonly TcpListener _listener;
    private readonly RotatingProxyApiSnapshotProvider _snapshotProvider;
    private readonly TimeSpan _readTimeout;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _connectionSlots = new(MaxConcurrentConnections, MaxConcurrentConnections);
    private readonly ConcurrentDictionary<long, Task> _clientTasks = new();
    private readonly CancellationTokenSource _disposeSource = new();
    private readonly TaskCompletionSource _startedSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _stoppedSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _clientId;
    private int _started;
    private int _disposed;

    public RotatingProxyApiServer(
        int apiPort,
        RotatingProxyApiSnapshotProvider snapshotProvider,
        TimeSpan? readTimeout = null,
        TimeProvider? timeProvider = null)
    {
        if (apiPort is < 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(apiPort));
        }
        ArgumentNullException.ThrowIfNull(snapshotProvider);
        if (readTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(readTimeout));
        }

        _listener = new TcpListener(IPAddress.Loopback, apiPort);
        _snapshotProvider = snapshotProvider;
        _readTimeout = readTimeout ?? TimeSpan.FromSeconds(5);
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public int Port { get; private set; }

    public async Task WaitUntilStartedAsync(CancellationToken cancellationToken = default)
    {
        await _startedSource.Task.WaitAsync(cancellationToken);
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("The rotating proxy API server has already been started.");
        }

        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _disposeSource.Token);
        using var stopRegistration = linkedSource.Token.Register(static state =>
        {
            try
            {
                ((TcpListener)state!).Stop();
            }
            catch
            {
                // Listener is already stopped.
            }
        }, _listener);

        try
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _startedSource.TrySetResult();

            while (!linkedSource.Token.IsCancellationRequested)
            {
                await _connectionSlots.WaitAsync(linkedSource.Token);
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(linkedSource.Token);
                }
                catch
                {
                    _connectionSlots.Release();
                    throw;
                }

                var clientId = Interlocked.Increment(ref _clientId);
                var task = Task.Run(
                    () => HandleClientAndReleaseAsync(client, linkedSource.Token),
                    CancellationToken.None);
                _clientTasks[clientId] = task;
                _ = RemoveCompletedClientAsync(clientId, task);
            }
        }
        catch (OperationCanceledException) when (linkedSource.Token.IsCancellationRequested)
        {
            // Normal service shutdown.
        }
        catch (SocketException) when (linkedSource.Token.IsCancellationRequested)
        {
            // TcpListener.Stop interrupts AcceptTcpClientAsync.
        }
        catch (Exception ex)
        {
            _startedSource.TrySetException(ex);
            throw;
        }
        finally
        {
            if (!_startedSource.Task.IsCompleted)
            {
                _startedSource.TrySetCanceled(linkedSource.Token);
            }
            try
            {
                _listener.Stop();
            }
            catch
            {
                // Listener is already stopped.
            }

            var clientTasks = _clientTasks.Values.ToArray();
            if (clientTasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(clientTasks);
                }
                catch (OperationCanceledException) when (linkedSource.Token.IsCancellationRequested)
                {
                    // Normal service shutdown.
                }
                catch
                {
                    // Individual connection failures are isolated from the listener loop.
                }
            }
            _stoppedSource.TrySetResult();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (!_disposeSource.IsCancellationRequested)
        {
            _disposeSource.Cancel();
        }
        try
        {
            _listener.Stop();
        }
        catch
        {
            // Listener was not started or is already stopped.
        }

        if (Volatile.Read(ref _started) != 0)
        {
            await _stoppedSource.Task;
        }
        else
        {
            _startedSource.TrySetCanceled(_disposeSource.Token);
            _stoppedSource.TrySetResult();
        }
        _disposeSource.Dispose();
        _connectionSlots.Dispose();
    }

    private async Task RemoveCompletedClientAsync(long clientId, Task task)
    {
        try
        {
            await task;
        }
        catch
        {
            // Handled inside the client task when a response can still be written.
        }
        finally
        {
            _clientTasks.TryRemove(clientId, out _);
        }
    }

    private async Task HandleClientAndReleaseAsync(TcpClient client, CancellationToken cancellationToken)
    {
        try
        {
            using (client)
            {
                client.NoDelay = true;
                await HandleClientAsync(client, cancellationToken);
            }
        }
        finally
        {
            _connectionSlots.Release();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var stream = client.GetStream();
        ApiRequest request;
        try
        {
            request = await ReadRequestAsync(stream, cancellationToken);
        }
        catch (ApiRequestException ex)
        {
            await TryWriteJsonAsync(stream, ex.StatusCode, ex.ReasonPhrase,
                CreateError(ex.ErrorCode, ex.Message, null), cancellationToken);
            return;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            Logging.SaveLog(nameof(RotatingProxyApiServer), ex);
            await TryWriteJsonAsync(stream, 400, "Bad Request",
                CreateError("bad_request", "The HTTP request could not be read.", null), cancellationToken);
            return;
        }

        if (!IsAllowedHost(request.Host))
        {
            await TryWriteJsonAsync(stream, 400, "Bad Request",
                CreateError("invalid_host", "A loopback Host header is required.", null), cancellationToken);
            return;
        }
        if (request.Method != "GET")
        {
            await TryWriteJsonAsync(stream, 405, "Method Not Allowed",
                CreateError("method_not_allowed", "Only GET is supported.", null), cancellationToken,
                ["Allow: GET"]);
            return;
        }
        var path = NormalizeRequestPath(request.Path);
        if (path is not ("/api/proxy/random" or "/api/proxy/status"))
        {
            await TryWriteJsonAsync(stream, 404, "Not Found",
                CreateError("not_found", "The requested endpoint does not exist.", null), cancellationToken);
            return;
        }

        RotatingProxyApiSnapshot snapshot;
        try
        {
            snapshot = _snapshotProvider()
                       ?? throw new InvalidOperationException("The snapshot provider returned null.");
        }
        catch (Exception ex)
        {
            Logging.SaveLog(nameof(RotatingProxyApiServer), ex);
            await TryWriteJsonAsync(stream, 503, "Service Unavailable",
                CreateError("core_unavailable", "The rotating proxy core state is unavailable.", null),
                cancellationToken);
            return;
        }

        var issuedAt = _timeProvider.GetUtcNow();
        var availability = GetAvailability(snapshot, issuedAt.UtcDateTime);
        if (path == "/api/proxy/status")
        {
            // Status is observational: always 200 when the API can answer, with ready=false when
            // credentials cannot currently be issued.
            await WriteStatusAsync(stream, snapshot, availability, issuedAt, cancellationToken);
            return;
        }
        if (availability is not null)
        {
            await TryWriteJsonAsync(stream, 503, "Service Unavailable",
                CreateError(availability.Code, availability.Message, snapshot.Generation, issuedAt),
                cancellationToken);
            return;
        }

        await WriteRandomProxyAsync(stream, snapshot, issuedAt, cancellationToken);
    }

    internal static string NormalizeRequestPath(string rawPath)
    {
        if (rawPath.IsNullOrEmpty())
        {
            return "/";
        }

        var path = rawPath;
        if (path.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            if (Uri.TryCreate(path, UriKind.Absolute, out var absolute))
            {
                path = absolute.AbsolutePath;
            }
        }

        var queryIndex = path.IndexOf('?', StringComparison.Ordinal);
        if (queryIndex >= 0)
        {
            path = path[..queryIndex];
        }
        var fragmentIndex = path.IndexOf('#', StringComparison.Ordinal);
        if (fragmentIndex >= 0)
        {
            path = path[..fragmentIndex];
        }
        if (path.IsNullOrEmpty())
        {
            return "/";
        }
        return path;
    }

    private async Task<ApiRequest> ReadRequestAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        using var timeoutSource = new CancellationTokenSource(_readTimeout, _timeProvider);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, timeoutSource.Token);
        using var buffer = new MemoryStream(MaxHeaderBytes);
        var readBuffer = new byte[1024];

        try
        {
            while (true)
            {
                var read = await stream.ReadAsync(readBuffer, linkedSource.Token);
                if (read == 0)
                {
                    throw new ApiRequestException(400, "Bad Request", "bad_request",
                        "The HTTP request headers are incomplete.");
                }

                buffer.Write(readBuffer, 0, read);
                var headerEnd = FindHeaderEnd(buffer.GetBuffer(), checked((int)buffer.Length));
                if (headerEnd >= 0)
                {
                    if (headerEnd > MaxHeaderBytes)
                    {
                        throw CreateHeaderTooLargeException();
                    }
                    return ParseRequest(buffer.GetBuffer().AsSpan(0, headerEnd));
                }
                if (buffer.Length > MaxHeaderBytes)
                {
                    throw CreateHeaderTooLargeException();
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new ApiRequestException(408, "Request Timeout", "request_timeout",
                "The HTTP request headers were not received in time.");
        }
    }

    private static ApiRequest ParseRequest(ReadOnlySpan<byte> headerBytes)
    {
        var lines = Encoding.ASCII.GetString(headerBytes)
            .Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length == 0)
        {
            throw new ApiRequestException(400, "Bad Request", "bad_request",
                "A valid HTTP/1.1 request line is required.");
        }
        var parts = lines[0].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3 || parts[2] != "HTTP/1.1")
        {
            throw new ApiRequestException(400, "Bad Request", "bad_request",
                "A valid HTTP/1.1 request line is required.");
        }

        string? host = null;
        foreach (var line in lines.Skip(1))
        {
            var separator = line.IndexOf(':');
            if (separator <= 0)
            {
                throw new ApiRequestException(400, "Bad Request", "bad_request",
                    "A valid HTTP header is required.");
            }
            if (!line[..separator].Equals("Host", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            if (host is not null)
            {
                throw new ApiRequestException(400, "Bad Request", "invalid_host",
                    "Exactly one Host header is required.");
            }
            host = line[(separator + 1)..].Trim();
        }
        if (host.IsNullOrEmpty())
        {
            throw new ApiRequestException(400, "Bad Request", "invalid_host",
                "Exactly one Host header is required.");
        }

        return new ApiRequest(parts[0], parts[1], host);
    }

    private bool IsAllowedHost(string host)
    {
        return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
               || host.Equals(Global.Loopback, StringComparison.Ordinal)
               || host.Equals($"localhost:{Port}", StringComparison.OrdinalIgnoreCase)
               || host.Equals($"{Global.Loopback}:{Port}", StringComparison.Ordinal);
    }

    private async Task WriteRandomProxyAsync(
        NetworkStream stream,
        RotatingProxyApiSnapshot snapshot,
        DateTimeOffset issuedAt,
        CancellationToken cancellationToken)
    {
        var credentialMap = snapshot.GetCredentialMap();
        var healthyNodes = GetHealthyNodes(snapshot, issuedAt.UtcDateTime)
            .Where(node => credentialMap.TryGetValue(node.IndexId, out var credential)
                           && credential.Username.IsNotEmpty()
                           && credential.Password.IsNotEmpty())
            .ToArray();
        if (healthyNodes.Length == 0)
        {
            await TryWriteJsonAsync(stream, 503, "Service Unavailable",
                CreateError("no_healthy_nodes", "No healthy rotating proxy node is available.",
                    snapshot.Generation, issuedAt), cancellationToken);
            return;
        }

        var node = healthyNodes[RandomNumberGenerator.GetInt32(healthyNodes.Length)];
        var credential = credentialMap[node.IndexId];
        var response = new
        {
            Proxy = new
            {
                Host = Global.Loopback,
                Port = snapshot.MixedPort,
                Protocols = _protocols,
                Username = credential.Username,
                Password = credential.Password,
            },
            Node = CreateNodeResponse(node),
            Generation = snapshot.Generation,
            IssuedAt = issuedAt,
            ExpiresAt = (DateTimeOffset?)null,
        };
        await TryWriteJsonAsync(stream, 200, "OK", response, cancellationToken);
    }

    private async Task WriteStatusAsync(
        NetworkStream stream,
        RotatingProxyApiSnapshot snapshot,
        ApiAvailability? availability,
        DateTimeOffset issuedAt,
        CancellationToken cancellationToken)
    {
        var oldestHealthyTime = GetOldestHealthyTime(snapshot, issuedAt.UtcDateTime);
        var response = new
        {
            Ready = availability is null,
            Error = availability is null
                ? null
                : new { availability.Code, availability.Message },
            Proxy = new
            {
                Host = Global.Loopback,
                Port = snapshot.MixedPort,
                Protocols = _protocols,
            },
            Service = new
            {
                State = snapshot.ServiceState.ToString(),
                snapshot.ProcessId,
                StartedAt = snapshot.StartedTime,
                LastError = snapshot.ServiceError.IsNotEmpty()
                    ? "The rotating proxy core reported an error."
                    : null,
            },
            Nodes = snapshot.GetNodes().Select(node => new
            {
                Id = node.IndexId,
                node.Remarks,
                node.SubscriptionId,
                node.SubscriptionRemarks,
                Type = node.ConfigType.ToString(),
                node.Address,
                node.Port,
                Healthy = oldestHealthyTime is not null
                          && node.State == ERotatingProxyNodeState.Healthy
                          && node.LastHealthyTime >= oldestHealthyTime,
                LatencyMs = node.Delay,
                LastCheckedAt = node.LastHealthCheckTime,
                LastHealthyAt = node.LastHealthyTime,
                ErrorType = node.LastErrorType?.ToString(),
                Error = node.LastError,
            }).ToArray(),
            Generation = snapshot.Generation,
            IssuedAt = issuedAt,
        };
        await TryWriteJsonAsync(stream, 200, "OK", response, cancellationToken);
    }

    private static object CreateNodeResponse(RotatingProxyNodeRuntime node)
    {
        return new
        {
            Id = node.IndexId,
            node.Remarks,
            node.SubscriptionId,
            node.SubscriptionRemarks,
            Type = node.ConfigType.ToString(),
            node.Address,
            node.Port,
            LatencyMs = node.Delay,
            LastCheckedAt = node.LastHealthCheckTime,
        };
    }

    private static ApiAvailability? GetAvailability(RotatingProxyApiSnapshot snapshot, DateTime utcNow)
    {
        if (snapshot.ServiceState == ERotatingProxyServiceState.Faulted)
        {
            return new ApiAvailability("core_unavailable", "The rotating proxy core is unavailable.");
        }
        if (!snapshot.IsReady)
        {
            return new ApiAvailability(
                "not_ready",
                snapshot.NotReadyReason.IsNotEmpty()
                    ? snapshot.NotReadyReason!
                    : "The rotating proxy service is not ready.");
        }
        if (snapshot.MixedPort is <= 0 or > 65535 || snapshot.HealthyMaxAgeSeconds <= 0)
        {
            return new ApiAvailability("not_ready", "The rotating proxy snapshot is incomplete.");
        }

        var credentialMap = snapshot.GetCredentialMap();
        var hasHealthyNode = GetHealthyNodes(snapshot, utcNow).Any(node =>
            credentialMap.TryGetValue(node.IndexId, out var credential)
            && credential.Username.IsNotEmpty()
            && credential.Password.IsNotEmpty());
        return hasHealthyNode
            ? null
            : new ApiAvailability("no_healthy_nodes", "No healthy rotating proxy node is available.");
    }

    private static IEnumerable<RotatingProxyNodeRuntime> GetHealthyNodes(
        RotatingProxyApiSnapshot snapshot,
        DateTime utcNow)
    {
        var oldestHealthyTime = GetOldestHealthyTime(snapshot, utcNow);
        if (oldestHealthyTime is null)
        {
            return [];
        }
        return snapshot.GetNodes().Where(node =>
            node.State == ERotatingProxyNodeState.Healthy
            && node.LastHealthyTime >= oldestHealthyTime);
    }

    private static DateTime? GetOldestHealthyTime(RotatingProxyApiSnapshot snapshot, DateTime utcNow)
    {
        return snapshot.HealthyMaxAgeSeconds > 0
            ? utcNow - TimeSpan.FromSeconds(snapshot.HealthyMaxAgeSeconds)
            : null;
    }

    private object CreateError(
        string code,
        string message,
        long? generation,
        DateTimeOffset? issuedAt = null)
    {
        return new
        {
            Error = new { Code = code, Message = message },
            Generation = generation,
            IssuedAt = issuedAt ?? _timeProvider.GetUtcNow(),
        };
    }

    private static async Task TryWriteJsonAsync(
        NetworkStream stream,
        int statusCode,
        string reasonPhrase,
        object payload,
        CancellationToken cancellationToken,
        IReadOnlyList<string>? extraHeaders = null)
    {
        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(payload, _jsonOptions);
            var headers = new StringBuilder()
                .Append("HTTP/1.1 ").Append(statusCode).Append(' ').Append(reasonPhrase).Append("\r\n")
                .Append("Content-Type: application/json; charset=utf-8\r\n")
                .Append("Content-Length: ").Append(body.Length).Append("\r\n")
                .Append("Cache-Control: no-store\r\n")
                .Append("Connection: close\r\n");
            foreach (var header in extraHeaders ?? [])
            {
                headers.Append(header).Append("\r\n");
            }
            headers.Append("\r\n");

            await stream.WriteAsync(Encoding.ASCII.GetBytes(headers.ToString()), cancellationToken);
            await stream.WriteAsync(body, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal service shutdown.
        }
        catch (IOException)
        {
            // Client disconnected before the response completed.
        }
        catch (SocketException)
        {
            // Client disconnected before the response completed.
        }
    }

    private static int FindHeaderEnd(byte[] buffer, int length)
    {
        for (var index = 3; index < length; index++)
        {
            if (buffer[index - 3] == '\r'
                && buffer[index - 2] == '\n'
                && buffer[index - 1] == '\r'
                && buffer[index] == '\n')
            {
                return index + 1;
            }
        }
        return -1;
    }

    private static ApiRequestException CreateHeaderTooLargeException()
    {
        return new ApiRequestException(431, "Request Header Fields Too Large", "headers_too_large",
            "The HTTP request headers exceed 16 KiB.");
    }

    private sealed record ApiRequest(string Method, string Path, string Host);
    private sealed record ApiAvailability(string Code, string Message);

    private sealed class ApiRequestException(
        int statusCode,
        string reasonPhrase,
        string errorCode,
        string message) : Exception(message)
    {
        public int StatusCode { get; } = statusCode;
        public string ReasonPhrase { get; } = reasonPhrase;
        public string ErrorCode { get; } = errorCode;
    }
}
