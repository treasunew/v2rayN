using AwesomeAssertions;
using Xunit;

namespace ServiceLib.Tests.Services;

public class ProcessServiceTests
{
    [Fact]
    public async Task WaitForExitAsync_ShouldExposeExitCodeSafely()
    {
        var (fileName, arguments) = CreateExitCommand(7);
        using var process = new ProcessService(
            fileName,
            arguments,
            Utils.GetTempPath(),
            displayLog: true,
            redirectInput: false,
            environmentVars: null,
            updateFunc: null);

        process.ExitCode.Should().BeNull();
        await process.StartAsync();
        await process.WaitForExitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

        process.HasExited.Should().BeTrue();
        process.ExitCode.Should().Be(7);
    }

    private static (string FileName, string Arguments) CreateExitCommand(int exitCode)
    {
        if (Utils.IsWindows())
        {
            return (Environment.GetEnvironmentVariable("COMSPEC") ?? "cmd.exe",
                $"/d /c exit {exitCode}");
        }

        return ("/bin/sh", $"-c \"exit {exitCode}\"");
    }
}
