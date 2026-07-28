using AwesomeAssertions;
using Xunit;

namespace ServiceLib.Tests.Services.RotatingProxy;

public class RotatingProxyHealthCheckerTests
{
    [Fact]
    public async Task CheckNowAsync_Success_ShouldMarkNodeHealthy()
    {
        var credential = CreateCredential("node-1");
        await using var proxy = new TestHttpProxy(new Dictionary<string, RotatingProxyCredential>
        {
            ["node-1"] = credential,
        });
        proxy.SetResponse(credential.Username, HttpStatusCode.NoContent);
        var checker = CreateChecker([CreateNode("node-1")], [credential], proxy.Port);

        await checker.CheckNowAsync();

        var snapshot = checker.GetSnapshot();
        snapshot.Should().ContainSingle();
        var node = snapshot.Single();
        node.State.Should().Be(ERotatingProxyNodeState.Healthy);
        node.Delay.Should().NotBeNull();
        node.LastHealthCheckTime.Should().NotBeNull();
        node.LastHealthyTime.Should().Be(node.LastHealthCheckTime);
        node.LastErrorType.Should().BeNull();
        node.LastError.Should().BeNull();
        checker.GetHealthyNodes().Should().ContainSingle();
        proxy.AuthorizedRequestCount.Should().Be(1);
    }

    [Fact]
    public async Task CheckNowAsync_FailedStatus_ShouldMarkNodeUnhealthy()
    {
        var credential = CreateCredential("node-1");
        await using var proxy = new TestHttpProxy(new Dictionary<string, RotatingProxyCredential>
        {
            ["node-1"] = credential,
        });
        proxy.SetResponse(credential.Username, HttpStatusCode.BadGateway);
        var checker = CreateChecker([CreateNode("node-1")], [credential], proxy.Port);

        await checker.CheckNowAsync();

        var snapshot = checker.GetSnapshot();
        snapshot.Should().ContainSingle();
        var node = snapshot.Single();
        node.State.Should().Be(ERotatingProxyNodeState.Unhealthy);
        node.LastErrorType.Should().Be(ERotatingProxyHealthErrorType.HttpStatus);
        node.LastError.Should().Contain("502");
        checker.GetHealthyNodes().Should().BeEmpty();
    }

    [Fact]
    public async Task CheckNowAsync_Timeout_ShouldRecordTimeout()
    {
        var credential = CreateCredential("node-1");
        await using var proxy = new TestHttpProxy(new Dictionary<string, RotatingProxyCredential>
        {
            ["node-1"] = credential,
        });
        proxy.SetResponse(credential.Username, HttpStatusCode.NoContent, TimeSpan.FromSeconds(5));
        var checker = CreateChecker([CreateNode("node-1")], [credential], proxy.Port,
            timeout: TimeSpan.FromMilliseconds(100));

        await checker.CheckNowAsync();

        var snapshot = checker.GetSnapshot();
        snapshot.Should().ContainSingle();
        var node = snapshot.Single();
        node.State.Should().Be(ERotatingProxyNodeState.Unhealthy);
        node.LastErrorType.Should().Be(ERotatingProxyHealthErrorType.Timeout);
        node.Delay.Should().BeGreaterThanOrEqualTo(50);
        checker.GetHealthyNodes().Should().BeEmpty();
    }

    [Fact]
    public async Task CheckNowAsync_ShouldRespectConcurrencyLimit()
    {
        var nodes = Enumerable.Range(1, 8).Select(index => CreateNode($"node-{index}")).ToArray();
        var credentials = nodes.Select(node => CreateCredential(node.IndexId)).ToArray();
        await using var proxy = new TestHttpProxy(nodes.Zip(credentials)
            .ToDictionary(item => item.First.IndexId, item => item.Second));
        foreach (var credential in credentials)
        {
            proxy.SetResponse(credential.Username, HttpStatusCode.NoContent, TimeSpan.FromMilliseconds(75));
        }
        var checker = CreateChecker(nodes, credentials, proxy.Port, concurrency: 2);

        await checker.CheckNowAsync();

        checker.GetHealthyNodes().Should().HaveCount(nodes.Length);
        proxy.AuthorizedRequestCount.Should().Be(nodes.Length);
        proxy.MaxActiveRequests.Should().BeLessThanOrEqualTo(2);
        proxy.MaxActiveRequests.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task CheckNowAsync_LatestFailure_ShouldImmediatelyRemovePreviouslyHealthyNode()
    {
        var credential = CreateCredential("node-1");
        await using var proxy = new TestHttpProxy(new Dictionary<string, RotatingProxyCredential>
        {
            ["node-1"] = credential,
        });
        proxy.SetResponse(credential.Username, HttpStatusCode.NoContent);
        var checker = CreateChecker([CreateNode("node-1")], [credential], proxy.Port);
        await checker.CheckNowAsync();
        var lastHealthyTime = checker.GetSnapshot().Single().LastHealthyTime;

        proxy.SetResponse(credential.Username, HttpStatusCode.BadGateway);
        await checker.CheckNowAsync();

        checker.GetHealthyNodes().Should().BeEmpty();
        var snapshot = checker.GetSnapshot();
        snapshot.Should().ContainSingle();
        var failedNode = snapshot.Single();
        failedNode.State.Should().Be(ERotatingProxyNodeState.Unhealthy);
        failedNode.LastHealthyTime.Should().Be(lastHealthyTime);
        failedNode.LastErrorType.Should().Be(ERotatingProxyHealthErrorType.HttpStatus);
    }

    [Fact]
    public async Task RunAsync_ShouldCheckImmediatelyAndRecoverOnPeriodicCheck()
    {
        var credential = CreateCredential("node-1");
        await using var proxy = new TestHttpProxy(new Dictionary<string, RotatingProxyCredential>
        {
            ["node-1"] = credential,
        });
        proxy.SetResponder(credential.Username, attempt =>
            attempt == 1
                ? new TestProxyResponse(HttpStatusCode.BadGateway, TimeSpan.Zero)
                : new TestProxyResponse(HttpStatusCode.NoContent, TimeSpan.Zero));
        var checker = CreateChecker([CreateNode("node-1")], [credential], proxy.Port,
            interval: TimeSpan.FromMilliseconds(100));
        using var cancellationSource = new CancellationTokenSource();

        var runTask = checker.RunAsync(cancellationSource.Token);
        await WaitUntilAsync(() => checker.GetSnapshot().Single().State == ERotatingProxyNodeState.Unhealthy);
        await WaitUntilAsync(() => checker.GetHealthyNodes().Count == 1);
        cancellationSource.Cancel();
        await runTask.WaitAsync(TimeSpan.FromSeconds(2));

        proxy.AuthorizedRequestCount.Should().BeGreaterThanOrEqualTo(2);
        var snapshot = checker.GetSnapshot();
        snapshot.Should().ContainSingle();
        var node = snapshot.Single();
        node.State.Should().Be(ERotatingProxyNodeState.Healthy);
        node.LastErrorType.Should().BeNull();
    }

    private static RotatingProxyHealthChecker CreateChecker(
        IReadOnlyList<RotatingProxyNodeRuntime> nodes,
        IReadOnlyList<RotatingProxyCredential> credentials,
        int proxyPort,
        TimeSpan? timeout = null,
        int concurrency = 4,
        TimeSpan? interval = null)
    {
        var credentialMap = nodes.Zip(credentials)
            .ToDictionary(item => item.First.IndexId, item => item.Second);
        return new RotatingProxyHealthChecker(
            nodes,
            credentialMap,
            proxyPort,
            "http://health-check.test/generate_204",
            timeout ?? TimeSpan.FromSeconds(2),
            concurrency,
            TimeSpan.FromMinutes(3),
            interval ?? TimeSpan.FromHours(1));
    }

    private static RotatingProxyNodeRuntime CreateNode(string indexId)
    {
        return new RotatingProxyNodeRuntime
        {
            IndexId = indexId,
            SubscriptionId = "subscription-1",
            Remarks = indexId,
            SubscriptionRemarks = "Subscription",
            OutboundTag = $"out-{indexId}",
            Username = $"user-{indexId}",
            ConfigType = EConfigType.SOCKS,
            Address = $"{indexId}.example.com",
            Port = 1080,
            State = ERotatingProxyNodeState.Unknown,
        };
    }

    private static RotatingProxyCredential CreateCredential(string indexId)
    {
        return new RotatingProxyCredential
        {
            Username = $"credential-{indexId}",
            Password = $"password-{indexId}",
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("The expected health state was not reached.");
            }
            await Task.Delay(10);
        }
    }

    private sealed class TestHttpProxy : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly IReadOnlyDictionary<string, RotatingProxyCredential> _credentials;
        private readonly ConcurrentDictionary<string, Func<int, TestProxyResponse>> _responders = new();
        private readonly ConcurrentDictionary<string, int> _attempts = new();
        private readonly ConcurrentDictionary<long, Task> _clientTasks = new();
        private readonly CancellationTokenSource _cancellationSource = new();
        private readonly Task _acceptTask;
        private long _clientId;
        private int _activeRequests;
        private int _maxActiveRequests;
        private int _authorizedRequestCount;

        public TestHttpProxy(IReadOnlyDictionary<string, RotatingProxyCredential> credentials)
        {
            _credentials = credentials.Values.ToDictionary(item => item.Username, item => item);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _acceptTask = AcceptLoopAsync();
        }

        public int Port { get; }
        public int AuthorizedRequestCount => Volatile.Read(ref _authorizedRequestCount);
        public int MaxActiveRequests => Volatile.Read(ref _maxActiveRequests);

        public void SetResponse(string username, HttpStatusCode statusCode, TimeSpan? delay = null)
        {
            SetResponder(username, _ => new TestProxyResponse(statusCode, delay ?? TimeSpan.Zero));
        }

        public void SetResponder(string username, Func<int, TestProxyResponse> responder)
        {
            _responders[username] = responder;
        }

        public async ValueTask DisposeAsync()
        {
            _cancellationSource.Cancel();
            _listener.Stop();
            try
            {
                await _acceptTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
            catch (SocketException)
            {
                // TcpListener.Stop interrupted accept.
            }

            var clientTasks = _clientTasks.Values.ToArray();
            if (clientTasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(clientTasks);
                }
                catch (OperationCanceledException)
                {
                    // Expected during shutdown.
                }
            }
            _cancellationSource.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            while (!_cancellationSource.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_cancellationSource.Token);
                var clientId = Interlocked.Increment(ref _clientId);
                var task = HandleClientAsync(client, _cancellationSource.Token);
                _clientTasks[clientId] = task;
                _ = RemoveClientAsync(clientId, task);
            }
        }

        private async Task RemoveClientAsync(long clientId, Task task)
        {
            try
            {
                await task;
            }
            catch
            {
                // Assertions inspect the health check result instead.
            }
            finally
            {
                _clientTasks.TryRemove(clientId, out _);
            }
        }

        private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
        {
            using (client)
            {
                var stream = client.GetStream();
                var requestHeader = await ReadHeadersAsync(stream, cancellationToken);
                var credential = GetCredential(requestHeader);
                if (credential is null
                    || !_credentials.TryGetValue(credential.Value.Username, out var expected)
                    || credential.Value.Password != expected.Password)
                {
                    await WriteResponseAsync(stream, 407, "Proxy Authentication Required", cancellationToken,
                        "Proxy-Authenticate: Basic realm=\"rotating-proxy-test\"\r\n");
                    return;
                }

                var active = Interlocked.Increment(ref _activeRequests);
                UpdateMaximum(ref _maxActiveRequests, active);
                Interlocked.Increment(ref _authorizedRequestCount);
                try
                {
                    var attempt = _attempts.AddOrUpdate(credential.Value.Username, 1, (_, current) => current + 1);
                    var response = _responders.TryGetValue(credential.Value.Username, out var responder)
                        ? responder(attempt)
                        : new TestProxyResponse(HttpStatusCode.NoContent, TimeSpan.Zero);
                    if (response.Delay > TimeSpan.Zero)
                    {
                        await Task.Delay(response.Delay, cancellationToken);
                    }
                    await WriteResponseAsync(stream, (int)response.StatusCode, response.StatusCode.ToString(),
                        cancellationToken);
                }
                finally
                {
                    Interlocked.Decrement(ref _activeRequests);
                }
            }
        }

        private static async Task<string> ReadHeadersAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            using var buffer = new MemoryStream();
            var bytes = new byte[1024];
            while (buffer.Length <= 16 * 1024)
            {
                var read = await stream.ReadAsync(bytes, cancellationToken);
                if (read == 0)
                {
                    break;
                }
                buffer.Write(bytes, 0, read);
                var text = Encoding.ASCII.GetString(buffer.GetBuffer(), 0, checked((int)buffer.Length));
                if (text.Contains("\r\n\r\n", StringComparison.Ordinal))
                {
                    return text;
                }
            }
            throw new InvalidDataException("Incomplete proxy request headers.");
        }

        private static (string Username, string Password)? GetCredential(string requestHeader)
        {
            var authorization = requestHeader.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)
                .FirstOrDefault(line => line.StartsWith("Proxy-Authorization: Basic ",
                    StringComparison.OrdinalIgnoreCase));
            if (authorization is null)
            {
                return null;
            }

            var value = authorization["Proxy-Authorization: Basic ".Length..].Trim();
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(value));
            var separator = decoded.IndexOf(':');
            return separator > 0
                ? (decoded[..separator], decoded[(separator + 1)..])
                : null;
        }

        private static async Task WriteResponseAsync(
            NetworkStream stream,
            int statusCode,
            string reasonPhrase,
            CancellationToken cancellationToken,
            string extraHeaders = "")
        {
            var response = Encoding.ASCII.GetBytes(
                $"HTTP/1.1 {statusCode} {reasonPhrase}\r\n{extraHeaders}Content-Length: 0\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(response, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }

        private static void UpdateMaximum(ref int target, int value)
        {
            var current = Volatile.Read(ref target);
            while (value > current)
            {
                var observed = Interlocked.CompareExchange(ref target, value, current);
                if (observed == current)
                {
                    return;
                }
                current = observed;
            }
        }
    }

    private sealed record TestProxyResponse(HttpStatusCode StatusCode, TimeSpan Delay);
}
