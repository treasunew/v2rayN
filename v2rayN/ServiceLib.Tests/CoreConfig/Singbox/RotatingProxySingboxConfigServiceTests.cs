using AwesomeAssertions;
using Xunit;

namespace ServiceLib.Tests.CoreConfig.Singbox;

public class RotatingProxySingboxConfigServiceTests
{
    [Fact]
    public void Generate_ShouldCreateMatchingUsersOutboundsAndRules()
    {
        var config = CoreConfigTestFactory.CreateConfig(ECoreType.sing_box);
        config.RotatingProxyItem.MixedPort = 20888;
        CoreConfigTestFactory.BindAppManagerConfig(config);
        var node1 = CoreConfigTestFactory.CreateSocksNode(ECoreType.sing_box, "node-1", "first remark");
        node1.Subid = "sub-1";
        var node2 = CoreConfigTestFactory.CreateHttpNode(ECoreType.sing_box, "node-2", "second remark");
        node2.Subid = "sub-2";
        var existingCredential = new RotatingProxyCredential
        {
            Username = "existing-user",
            Password = "existing-password",
        };

        var result = new RotatingProxySingboxConfigService().Generate(
            config,
            [node1, node2],
            new Dictionary<string, string>
            {
                ["sub-1"] = "Subscription One",
                ["sub-2"] = "Subscription Two",
            },
            new Dictionary<string, RotatingProxyCredential>
            {
                [node1.IndexId] = existingCredential,
            });

        result.Success.Should().BeTrue();
        result.Nodes.Should().HaveCount(2);
        result.CredentialMap.Should().ContainKeys(node1.IndexId, node2.IndexId);
        result.CredentialMap[node1.IndexId].Username.Should().Be(existingCredential.Username);
        result.CredentialMap[node1.IndexId].Password.Should().Be(existingCredential.Password);
        result.CredentialMap[node2.IndexId].Username.Should().NotBeNullOrEmpty();
        result.CredentialMap[node2.IndexId].Password.Should().NotBeNullOrEmpty();
        result.CredentialMap[node2.IndexId].Password.Should().NotBe(node2.Password);
        result.Nodes.Single(t => t.IndexId == node1.IndexId).SubscriptionRemarks.Should().Be("Subscription One");
        result.Nodes.Single(t => t.IndexId == node2.IndexId).SubscriptionRemarks.Should().Be("Subscription Two");
        var runtimeNode = result.Nodes.Single(t => t.IndexId == node1.IndexId);
        runtimeNode.ConfigType.Should().Be(node1.ConfigType);
        runtimeNode.Address.Should().Be(node1.Address);
        runtimeNode.Port.Should().Be(node1.Port);

        var coreConfig = JsonUtils.Deserialize<SingboxConfig>(result.ConfigJson)!;
        coreConfig.experimental.Should().BeNull();
        coreConfig.inbounds.Should().ContainSingle();
        var inbound = coreConfig.inbounds.Single();
        inbound.type.Should().Be(nameof(EInboundProtocol.mixed));
        inbound.listen.Should().Be(Global.Loopback);
        inbound.listen_port.Should().Be(20888);
        inbound.users.Should().HaveCount(2);
        coreConfig.outbounds.Should().HaveCount(2);
        coreConfig.outbounds.Should().OnlyContain(t => t.type != "selector" && t.type != "urltest");

        foreach (var node in result.Nodes)
        {
            var credential = result.CredentialMap[node.IndexId];
            inbound.users.Should().ContainSingle(t =>
                t.username == credential.Username && t.password == credential.Password);
            coreConfig.outbounds.Should().ContainSingle(t => t.tag == node.OutboundTag);
            coreConfig.route.rules.Should().ContainSingle(t =>
                t.auth_user != null
                && t.auth_user.SequenceEqual(new[] { credential.Username })
                && t.action == "route"
                && t.outbound == node.OutboundTag);
            node.OutboundTag.Should().NotContain(node.Remarks);
        }

        var rejectRuleIndex = coreConfig.route.rules.FindIndex(t => t.action == "reject");
        rejectRuleIndex.Should().Be(coreConfig.route.rules.Count - 1);
        coreConfig.route.rules.Take(rejectRuleIndex).Should().OnlyContain(t => t.auth_user?.Count == 1);
    }

    [Fact]
    public void Generate_ShouldFilterInvalidUnsupportedComplexAndDuplicateNodes()
    {
        var config = CoreConfigTestFactory.CreateConfig(ECoreType.sing_box);
        CoreConfigTestFactory.BindAppManagerConfig(config);
        var valid = CoreConfigTestFactory.CreateSocksNode(ECoreType.sing_box, "node-valid", "valid");
        var duplicate = CoreConfigTestFactory.CreateHttpNode(ECoreType.sing_box, valid.IndexId, "duplicate");
        var invalid = CoreConfigTestFactory.CreateSocksNode(ECoreType.sing_box, "node-invalid", "invalid");
        invalid.Address = string.Empty;
        var unsupported = CoreConfigTestFactory.CreateSocksNode(ECoreType.sing_box, "node-unsupported", "unsupported");
        unsupported.ConfigType = (EConfigType)999;
        var custom = new ProfileItem
        {
            IndexId = "node-custom",
            ConfigType = EConfigType.Custom,
            Remarks = "custom",
            Address = "custom.json",
        };
        var complex = CoreConfigTestFactory.CreatePolicyGroupNode(ECoreType.sing_box, "node-group", "group",
            [valid.IndexId]);
        var emptyIndex = CoreConfigTestFactory.CreateSocksNode(ECoreType.sing_box, string.Empty, "empty-index");

        var result = new RotatingProxySingboxConfigService().Generate(config,
            [valid, duplicate, invalid, unsupported, custom, complex, emptyIndex]);

        result.Success.Should().BeTrue();
        result.Nodes.Should().ContainSingle(t => t.IndexId == valid.IndexId);
        var skipReasons = result.Skipped.Select(t => t.Reason).ToList();
        skipReasons.Should().Contain(ERotatingProxySkipReason.DuplicateIndexId);
        skipReasons.Should().Contain(ERotatingProxySkipReason.Invalid);
        skipReasons.Should().Contain(ERotatingProxySkipReason.UnsupportedConfigType);
        skipReasons.Should().Contain(ERotatingProxySkipReason.Custom);
        skipReasons.Should().Contain(ERotatingProxySkipReason.Complex);
        skipReasons.Should().Contain(ERotatingProxySkipReason.EmptyIndexId);
    }

    [Fact]
    public void Generate_WireGuardNode_ShouldCreateEndpointAndRouteRule()
    {
        var config = CoreConfigTestFactory.CreateConfig(ECoreType.sing_box);
        CoreConfigTestFactory.BindAppManagerConfig(config);
        var node = CoreConfigTestFactory.CreateWireGuardNode(ECoreType.sing_box);

        var result = new RotatingProxySingboxConfigService().Generate(config, [node]);

        result.Success.Should().BeTrue();
        result.Nodes.Should().ContainSingle();
        var generatedNode = result.Nodes.Single();
        var coreConfig = JsonUtils.Deserialize<SingboxConfig>(result.ConfigJson)!;
        coreConfig.outbounds.Should().BeEmpty();
        coreConfig.endpoints.Should().ContainSingle();
        var endpoint = coreConfig.endpoints!.Single();
        endpoint.tag.Should().Be(generatedNode.OutboundTag);
        endpoint.type.Should().Be("wireguard");
        endpoint.private_key.Should().Be("private-key");
        endpoint.address.Should().BeEquivalentTo(new[] { "10.0.0.2/32", "fd00::2/128" });
        endpoint.mtu.Should().Be(1420);
        endpoint.peers.Should().ContainSingle();
        endpoint.peers.Single().address.Should().Be("wg.example.com");
        endpoint.peers.Single().port.Should().Be(51820);
        endpoint.peers.Single().public_key.Should().Be("public-key");
        endpoint.peers.Single().reserved.Should().BeEquivalentTo(new[] { 1, 2, 3 });
        coreConfig.route.rules.Should().ContainSingle(t =>
            t.auth_user != null
            && t.auth_user.SequenceEqual(new[] { generatedNode.Username })
            && t.action == "route"
            && t.outbound == generatedNode.OutboundTag);
    }
}
