namespace ServiceLib.Models.Dto;

public class RotatingProxyCredential
{
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

public class RotatingProxySubscriptionOption : ReactiveObject
{
    public string Id { get; init; } = string.Empty;
    public string Remarks { get; init; } = string.Empty;
    public bool Enabled { get; init; }
    [Reactive] public bool IsSelected { get; set; }
}

public class RotatingProxyCandidate
{
    public string IndexId { get; set; } = string.Empty;
    public string SubscriptionId { get; set; } = string.Empty;
    public string Remarks { get; set; } = string.Empty;
    public string SubscriptionRemarks { get; set; } = string.Empty;
}

public class RotatingProxyNodeRuntime : RotatingProxyCandidate
{
    public string OutboundTag { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public EConfigType ConfigType { get; set; }
    public string Address { get; set; } = string.Empty;
    public int Port { get; set; }
    public ERotatingProxyNodeState State { get; set; }
    public int? Delay { get; set; }
    public DateTime? LastHealthCheckTime { get; set; }
    public DateTime? LastHealthyTime { get; set; }
    public ERotatingProxyHealthErrorType? LastErrorType { get; set; }
    public string? LastError { get; set; }
}

public class RotatingProxySkippedNode : RotatingProxyCandidate
{
    public ERotatingProxySkipReason Reason { get; set; }
}

public class RotatingProxyGenerationResult
{
    public bool Success { get; set; }
    public string ConfigJson { get; set; } = string.Empty;
    public string? ErrorMessage { get; set; }
    public List<RotatingProxyNodeRuntime> Nodes { get; set; } = [];
    public List<RotatingProxySkippedNode> Skipped { get; set; } = [];
    public Dictionary<string, RotatingProxyCredential> CredentialMap { get; set; } = [];
}

public class RotatingProxyServiceStatus
{
    public ERotatingProxyServiceState State { get; set; }
    public int ProcessId { get; set; }
    public DateTime? StartedTime { get; set; }
    public string? LastError { get; set; }
}

public class RotatingProxySnapshot
{
    public DateTime CreatedTime { get; set; } = DateTime.UtcNow;
    public bool IsReady { get; set; }
    public long Generation { get; set; }
    public RotatingProxyServiceStatus Service { get; set; } = new();
    public List<RotatingProxyNodeRuntime> Nodes { get; set; } = [];
    public List<RotatingProxySkippedNode> Skipped { get; set; } = [];
}

public sealed class RotatingProxyApiSnapshot
{
    private readonly RotatingProxyNodeRuntime[] _nodes;
    private readonly Dictionary<string, RotatingProxyCredential> _credentialMap;

    public RotatingProxyApiSnapshot(
        bool isReady,
        long generation,
        int mixedPort,
        int healthyMaxAgeSeconds,
        RotatingProxyServiceStatus service,
        IEnumerable<RotatingProxyNodeRuntime> nodes,
        IReadOnlyDictionary<string, RotatingProxyCredential> credentialMap,
        string? notReadyReason = null)
    {
        ArgumentNullException.ThrowIfNull(service);
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(credentialMap);

        IsReady = isReady;
        Generation = generation;
        MixedPort = mixedPort;
        HealthyMaxAgeSeconds = healthyMaxAgeSeconds;
        ServiceState = service.State;
        ProcessId = service.ProcessId;
        StartedTime = service.StartedTime;
        ServiceError = service.LastError;
        NotReadyReason = notReadyReason;
        _nodes = nodes.Select(CloneNode).ToArray();
        _credentialMap = credentialMap.ToDictionary(
            item => item.Key,
            item => new RotatingProxyCredential
            {
                Username = item.Value.Username,
                Password = item.Value.Password,
            });
    }

    public bool IsReady { get; }
    public long Generation { get; }
    public int MixedPort { get; }
    public int HealthyMaxAgeSeconds { get; }
    public ERotatingProxyServiceState ServiceState { get; }
    public int ProcessId { get; }
    public DateTime? StartedTime { get; }
    public string? ServiceError { get; }
    public string? NotReadyReason { get; }

    public IReadOnlyList<RotatingProxyNodeRuntime> GetNodes()
    {
        return _nodes.Select(CloneNode).ToArray();
    }

    public IReadOnlyDictionary<string, RotatingProxyCredential> GetCredentialMap()
    {
        return _credentialMap.ToDictionary(
            item => item.Key,
            item => new RotatingProxyCredential
            {
                Username = item.Value.Username,
                Password = item.Value.Password,
            });
    }

    private static RotatingProxyNodeRuntime CloneNode(RotatingProxyNodeRuntime node)
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
