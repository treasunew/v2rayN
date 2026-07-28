using AwesomeAssertions;
using Xunit;

namespace ServiceLib.Tests.Services.RotatingProxy;

public class RotatingProxyApiServerTests
{
    [Fact]
    public async Task RandomEndpoint_ShouldReturnProxyAndNodeWithoutCachingOrCors()
    {
        await using var api = new TestApiServer(() => CreateSnapshot());

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/random HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(200);
        response.Headers["Cache-Control"].Should().Be("no-store");
        response.Headers["Connection"].Should().Be("close");
        response.Headers["Content-Type"].Should().Be("application/json; charset=utf-8");
        response.Headers.Keys.Should().NotContain(header =>
            header.StartsWith("Access-Control-", StringComparison.OrdinalIgnoreCase));
        using var document = JsonDocument.Parse(response.Body);
        var root = document.RootElement;
        root.GetProperty("proxy").GetProperty("host").GetString().Should().Be(Global.Loopback);
        root.GetProperty("proxy").GetProperty("port").GetInt32().Should().Be(20808);
        root.GetProperty("proxy").GetProperty("protocols").EnumerateArray()
            .Select(item => item.GetString()).Should().BeEquivalentTo(["http", "socks5"]);
        root.GetProperty("proxy").GetProperty("username").GetString().Should().Be("local-user");
        root.GetProperty("proxy").GetProperty("password").GetString().Should().Be("local-password");
        root.GetProperty("node").GetProperty("id").GetString().Should().Be("node-1");
        root.GetProperty("node").GetProperty("remarks").GetString().Should().Be("Node One");
        root.GetProperty("node").GetProperty("subscriptionId").GetString().Should().Be("sub-1");
        root.GetProperty("node").GetProperty("subscriptionRemarks").GetString().Should().Be("Subscription One");
        root.GetProperty("node").GetProperty("type").GetString().Should().Be(nameof(EConfigType.SOCKS));
        root.GetProperty("node").GetProperty("address").GetString().Should().Be("proxy.example.com");
        root.GetProperty("node").GetProperty("port").GetInt32().Should().Be(1080);
        root.GetProperty("node").GetProperty("latencyMs").GetInt32().Should().Be(25);
        root.GetProperty("node").TryGetProperty("lastCheckedAt", out _).Should().BeTrue();
        root.GetProperty("generation").GetInt64().Should().Be(7);
        root.TryGetProperty("issuedAt", out _).Should().BeTrue();
        root.GetProperty("expiresAt").ValueKind.Should().Be(JsonValueKind.Null);
        response.Body.Should().NotContain("upstream-password");
        response.Body.Should().NotContain("upstream-uuid");
    }

    [Fact]
    public async Task StatusEndpoint_ShouldReturnRuntimeStateWithoutCredentials()
    {
        await using var api = new TestApiServer(() => CreateSnapshot());

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/status HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(200);
        using var document = JsonDocument.Parse(response.Body);
        var root = document.RootElement;
        root.GetProperty("ready").GetBoolean().Should().BeTrue();
        root.GetProperty("service").GetProperty("state").GetString()
            .Should().Be(nameof(ERotatingProxyServiceState.Running));
        root.GetProperty("nodes").GetArrayLength().Should().Be(1);
        root.GetProperty("nodes")[0].GetProperty("healthy").GetBoolean().Should().BeTrue();
        response.Body.Should().NotContain("local-user");
        response.Body.Should().NotContain("local-password");
    }

    [Fact]
    public async Task UnknownEndpoint_ShouldReturn404WithoutCallingProvider()
    {
        var providerCalls = 0;
        await using var api = new TestApiServer(() =>
        {
            Interlocked.Increment(ref providerCalls);
            return CreateSnapshot();
        });

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/missing HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(404);
        providerCalls.Should().Be(0);
        response.Headers["Cache-Control"].Should().Be("no-store");
        GetErrorCode(response).Should().Be("not_found");
    }

    [Fact]
    public async Task NonGetRequest_ShouldReturn405AndAllowHeader()
    {
        await using var api = new TestApiServer(() => CreateSnapshot());

        var response = await SendRequestAsync(api.Port, "POST /api/proxy/random HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");

        response.StatusCode.Should().Be(405);
        response.Headers["Allow"].Should().Be("GET");
        response.Headers["Cache-Control"].Should().Be("no-store");
        GetErrorCode(response).Should().Be("method_not_allowed");
    }

    [Theory]
    [InlineData("")]
    [InlineData("Host: proxy.example.com\r\n")]
    [InlineData("Host: localhost\r\nHost: localhost\r\n")]
    public async Task InvalidHostHeader_ShouldReturn400(string hostHeaders)
    {
        await using var api = new TestApiServer(() => CreateSnapshot());

        var response = await SendRequestAsync(api.Port,
            $"GET /api/proxy/random HTTP/1.1\r\n{hostHeaders}\r\n");

        response.StatusCode.Should().Be(400);
        GetErrorCode(response).Should().Be("invalid_host");
    }

    [Theory]
    [InlineData("not-ready", "not_ready")]
    [InlineData("no-healthy", "no_healthy_nodes")]
    [InlineData("core-fault", "core_unavailable")]
    public async Task RandomEndpoint_UnavailableSnapshot_ShouldReturnStructured503(
        string mode,
        string expectedCode)
    {
        await using var api = new TestApiServer(() => mode switch
        {
            "not-ready" => CreateSnapshot(isReady: false, notReadyReason: "Starting core."),
            "no-healthy" => CreateSnapshot(nodeState: ERotatingProxyNodeState.Unhealthy),
            "core-fault" => CreateSnapshot(serviceState: ERotatingProxyServiceState.Faulted,
                serviceError: "Core exited."),
            _ => throw new InvalidOperationException(),
        });

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/random HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(503);
        response.Headers["Cache-Control"].Should().Be("no-store");
        GetErrorCode(response).Should().Be(expectedCode);
    }

    [Fact]
    public async Task StatusEndpoint_UnavailableSnapshot_ShouldStillReturn200WithReadyFalse()
    {
        await using var api = new TestApiServer(() =>
            CreateSnapshot(isReady: false, notReadyReason: "Starting core."));

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/status HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(200);
        using var document = JsonDocument.Parse(response.Body);
        document.RootElement.GetProperty("ready").GetBoolean().Should().BeFalse();
        document.RootElement.GetProperty("error").GetProperty("code").GetString().Should().Be("not_ready");
    }

    [Theory]
    [InlineData("/api/proxy/random?client=test")]
    [InlineData("/api/proxy/status?full=1")]
    public async Task RequestPath_WithQueryString_ShouldStillRoute(string path)
    {
        await using var api = new TestApiServer(() => CreateSnapshot());

        var response = await SendRequestAsync(api.Port, $"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(200);
    }

    [Theory]
    [InlineData("/api/proxy/random?client=test", "/api/proxy/random")]
    [InlineData("/api/proxy/status#section", "/api/proxy/status")]
    [InlineData("http://127.0.0.1:20809/api/proxy/random?x=1", "/api/proxy/random")]
    public void NormalizeRequestPath_ShouldStripQueryFragmentAndAbsoluteForm(
        string raw,
        string expected)
    {
        RotatingProxyApiServer.NormalizeRequestPath(raw).Should().Be(expected);
    }

    [Fact]
    public async Task ProviderException_ShouldReturnCoreUnavailable503()
    {
        await using var api = new TestApiServer(() => throw new InvalidOperationException("Core state failed."));

        var response = await SendRequestAsync(api.Port, "GET /api/proxy/random HTTP/1.1\r\nHost: localhost\r\n\r\n");

        response.StatusCode.Should().Be(503);
        GetErrorCode(response).Should().Be("core_unavailable");
    }

    [Fact]
    public async Task WaitUntilStartedAsync_ShouldPropagatePortBindingFailure()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var server = new RotatingProxyApiServer(port, () => CreateSnapshot());
        try
        {
            var runTask = server.RunAsync();

            await Assert.ThrowsAsync<SocketException>(() => server.WaitUntilStartedAsync());
            await Assert.ThrowsAsync<SocketException>(() => runTask);
        }
        finally
        {
            listener.Stop();
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConcurrentRequests_ShouldNotExceedConnectionLimit()
    {
        var activeProviders = 0;
        var maxActiveProviders = 0;
        await using var api = new TestApiServer(() =>
        {
            var active = Interlocked.Increment(ref activeProviders);
            UpdateMaximum(ref maxActiveProviders, active);
            try
            {
                Thread.Sleep(40);
                return CreateSnapshot();
            }
            finally
            {
                Interlocked.Decrement(ref activeProviders);
            }
        });

        var requests = Enumerable.Range(0, 80)
            .Select(_ => SendRequestAsync(api.Port,
                "GET /api/proxy/random HTTP/1.1\r\nHost: localhost\r\n\r\n"));
        var responses = await Task.WhenAll(requests);

        responses.Should().OnlyContain(response => response.StatusCode == 200);
        maxActiveProviders.Should().BeGreaterThan(0);
        maxActiveProviders.Should().BeLessThanOrEqualTo(64);
    }

    private static RotatingProxyApiSnapshot CreateSnapshot(
        bool isReady = true,
        ERotatingProxyNodeState nodeState = ERotatingProxyNodeState.Healthy,
        ERotatingProxyServiceState serviceState = ERotatingProxyServiceState.Running,
        string? notReadyReason = null,
        string? serviceError = null)
    {
        var now = DateTime.UtcNow;
        var node = new RotatingProxyNodeRuntime
        {
            IndexId = "node-1",
            SubscriptionId = "sub-1",
            Remarks = "Node One",
            SubscriptionRemarks = "Subscription One",
            OutboundTag = "out-node-1",
            Username = "local-user",
            ConfigType = EConfigType.SOCKS,
            Address = "proxy.example.com",
            Port = 1080,
            State = nodeState,
            Delay = 25,
            LastHealthCheckTime = now,
            LastHealthyTime = now,
            LastErrorType = nodeState == ERotatingProxyNodeState.Healthy
                ? null
                : ERotatingProxyHealthErrorType.HttpStatus,
            LastError = nodeState == ERotatingProxyNodeState.Healthy ? null : "HTTP 502.",
        };
        return new RotatingProxyApiSnapshot(
            isReady,
            7,
            20808,
            180,
            new RotatingProxyServiceStatus
            {
                State = serviceState,
                ProcessId = 1234,
                StartedTime = now.AddMinutes(-1),
                LastError = serviceError,
            },
            [node],
            new Dictionary<string, RotatingProxyCredential>
            {
                [node.IndexId] = new()
                {
                    Username = "local-user",
                    Password = "local-password",
                },
            },
            notReadyReason);
    }

    private static async Task<TestApiResponse> SendRequestAsync(int port, string request)
    {
        using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port, timeoutSource.Token);
        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request), timeoutSource.Token);
        await stream.FlushAsync(timeoutSource.Token);

        using var responseBuffer = new MemoryStream();
        var buffer = new byte[4096];
        while (true)
        {
            var read = await stream.ReadAsync(buffer, timeoutSource.Token);
            if (read == 0)
            {
                break;
            }
            responseBuffer.Write(buffer, 0, read);
        }

        var responseBytes = responseBuffer.ToArray();
        var headerEnd = FindHeaderEnd(responseBytes);
        headerEnd.Should().BeGreaterThan(0);
        var headerText = Encoding.ASCII.GetString(responseBytes, 0, headerEnd);
        var lines = headerText.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        var statusParts = lines[0].Split(' ', 3);
        var headers = lines.Skip(1)
            .Select(line => line.Split(':', 2))
            .ToDictionary(parts => parts[0], parts => parts[1].Trim(), StringComparer.OrdinalIgnoreCase);
        var bodyOffset = headerEnd + 4;
        var body = Encoding.UTF8.GetString(responseBytes, bodyOffset, responseBytes.Length - bodyOffset);
        return new TestApiResponse(int.Parse(statusParts[1]), headers, body);
    }

    private static string GetErrorCode(TestApiResponse response)
    {
        using var document = JsonDocument.Parse(response.Body);
        return document.RootElement.GetProperty("error").GetProperty("code").GetString()!;
    }

    private static int FindHeaderEnd(byte[] response)
    {
        for (var index = 3; index < response.Length; index++)
        {
            if (response[index - 3] == '\r'
                && response[index - 2] == '\n'
                && response[index - 1] == '\r'
                && response[index] == '\n')
            {
                return index - 3;
            }
        }
        return -1;
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

    private sealed class TestApiServer : IAsyncDisposable
    {
        private readonly RotatingProxyApiServer _server;
        private readonly CancellationTokenSource _cancellationSource = new();
        private readonly Task _runTask;

        public TestApiServer(RotatingProxyApiSnapshotProvider snapshotProvider)
        {
            _server = new RotatingProxyApiServer(0, snapshotProvider, TimeSpan.FromSeconds(2));
            _runTask = _server.RunAsync(_cancellationSource.Token);
            _server.WaitUntilStartedAsync(_cancellationSource.Token).GetAwaiter().GetResult();
            Port = _server.Port;
        }

        public int Port { get; }

        public async ValueTask DisposeAsync()
        {
            _cancellationSource.Cancel();
            await _runTask.WaitAsync(TimeSpan.FromSeconds(5));
            await _server.DisposeAsync();
            _cancellationSource.Dispose();
        }
    }

    private sealed record TestApiResponse(
        int StatusCode,
        IReadOnlyDictionary<string, string> Headers,
        string Body);
}
