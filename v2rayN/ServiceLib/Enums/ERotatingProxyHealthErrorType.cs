namespace ServiceLib.Enums;

public enum ERotatingProxyHealthErrorType
{
    MissingCredential = 0,
    Timeout = 1,
    HttpStatus = 2,
    HttpRequest = 3,
    Unexpected = 4,
}
