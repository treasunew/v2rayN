using AwesomeAssertions;
using Xunit;

namespace ServiceLib.Tests.Manager;

public class RotatingProxyManagerTests
{
    [Fact]
    public async Task DisabledManager_ShouldInitializeAndStopIdempotently()
    {
        var manager = new RotatingProxyManager();
        var config = new Config
        {
            RotatingProxyItem = new RotatingProxyItem
            {
                Enabled = false,
            },
        };

        await manager.InitializeAsync(config, (_, _) => Task.CompletedTask);
        manager.GetStatus().State.Should().Be(ERotatingProxyServiceState.Disabled);
        manager.GetSnapshot().IsReady.Should().BeFalse();

        await manager.StopAsync();
        await manager.StopAsync();

        var snapshot = manager.GetSnapshot();
        snapshot.IsReady.Should().BeFalse();
        snapshot.Service.State.Should().Be(ERotatingProxyServiceState.Stopped);
        snapshot.Nodes.Should().BeEmpty();
        snapshot.Skipped.Should().BeEmpty();
    }

    [Fact]
    public void InitialSnapshot_ShouldBeNotReadyAndDefensivelyCopied()
    {
        var manager = new RotatingProxyManager();

        var first = manager.GetSnapshot();
        first.IsReady.Should().BeFalse();
        first.Generation.Should().Be(0);
        first.Service.State.Should().Be(ERotatingProxyServiceState.Stopped);
        first.Nodes.Add(new RotatingProxyNodeRuntime { IndexId = "mutated" });

        manager.GetSnapshot().Nodes.Should().BeEmpty();
        manager.SnapshotProvider().IsReady.Should().BeFalse();
    }

    [Theory]
    [InlineData(true, new[] { "sub-1", "sub-2" }, new[] { "sub-2" })]
    [InlineData(false, new[] { "sub-1" }, new[] { "sub-2" })]
    [InlineData(false, new string[0], new[] { "sub-1" })]
    [InlineData(false, new[] { "sub-1" }, new string[0])]
    public void HasSelectedSubscriptionIntersection_ShouldMatchOnlySelectedUpdates(
        bool expected,
        string[] selected,
        string[] updated)
    {
        RotatingProxyManager.HasSelectedSubscriptionIntersection(selected, updated)
            .Should().Be(expected);
    }
}
