namespace ServiceLib.Enums;

public enum ERotatingProxySkipReason
{
    Invalid = 0,
    EmptyIndexId = 1,
    DuplicateIndexId = 2,
    Custom = 3,
    Complex = 4,
    UnsupportedConfigType = 5,
    OutboundGenerationFailed = 6,
    SubscriptionMissing = 7,
    SubscriptionDisabled = 8,
}
