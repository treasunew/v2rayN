namespace ServiceLib.Enums;

public enum ERotatingProxyServiceState
{
    Stopped = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Faulted = 4,
    Disabled = 5,
    Degraded = 6,
    Rebuilding = 7,
}
