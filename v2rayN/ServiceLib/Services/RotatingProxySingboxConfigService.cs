namespace ServiceLib.Services;

public class RotatingProxySingboxConfigService
{
    private const string InboundTag = "rotating-proxy-in";
    private const string OutboundTagPrefix = "rotating-proxy-out-";

    public RotatingProxyGenerationResult Generate(
        Config config,
        IEnumerable<ProfileItem>? candidates,
        IReadOnlyDictionary<string, string>? subscriptionRemarks = null,
        IReadOnlyDictionary<string, RotatingProxyCredential>? credentialMap = null)
    {
        ArgumentNullException.ThrowIfNull(config);

        var result = new RotatingProxyGenerationResult
        {
            CredentialMap = CloneCredentialMap(credentialMap),
        };
        var acceptedIndexIds = new HashSet<string>();
        var usedOutboundTags = new HashSet<string>();
        var usedUsernames = new HashSet<string>();
        var servers = new List<BaseServer4Sbox>();
        var users = new List<User4Sbox>();
        var rules = new List<Rule4Sbox>();
        ProfileItem? baseNode = null;

        foreach (var node in candidates ?? [])
        {
            var candidate = CreateCandidate(node, subscriptionRemarks);
            var skipReason = GetSkipReason(node, acceptedIndexIds);
            if (skipReason is not null)
            {
                result.Skipped.Add(CreateSkippedNode(candidate, skipReason.Value));
                continue;
            }

            acceptedIndexIds.Add(node.IndexId);
            var outboundTag = CreateOutboundTag(node.IndexId, usedOutboundTags);
            List<BaseServer4Sbox> nodeServers;
            try
            {
                nodeServers = CoreConfigSingboxService.BuildLeafProxyOutbounds(config, node, outboundTag);
            }
            catch (Exception ex)
            {
                Logging.SaveLog(nameof(RotatingProxySingboxConfigService), ex);
                result.Skipped.Add(CreateSkippedNode(candidate, ERotatingProxySkipReason.OutboundGenerationFailed));
                continue;
            }
            if (nodeServers.Count != 1 || !IsGeneratedServerValid(nodeServers[0], outboundTag))
            {
                result.Skipped.Add(CreateSkippedNode(candidate, ERotatingProxySkipReason.OutboundGenerationFailed));
                continue;
            }

            var credential = GetOrCreateCredential(node.IndexId, result.CredentialMap, usedUsernames);
            baseNode ??= node;
            servers.AddRange(nodeServers);
            users.Add(new User4Sbox
            {
                username = credential.Username,
                password = credential.Password,
            });
            rules.Add(new Rule4Sbox
            {
                inbound = [InboundTag],
                auth_user = [credential.Username],
                action = "route",
                outbound = outboundTag,
            });
            result.Nodes.Add(new RotatingProxyNodeRuntime
            {
                IndexId = candidate.IndexId,
                SubscriptionId = candidate.SubscriptionId,
                Remarks = candidate.Remarks,
                SubscriptionRemarks = candidate.SubscriptionRemarks,
                OutboundTag = outboundTag,
                Username = credential.Username,
                ConfigType = node.ConfigType,
                Address = node.Address,
                Port = node.Port,
                State = ERotatingProxyNodeState.Unknown,
            });
        }

        if (result.Nodes.Count == 0)
        {
            return result;
        }

        try
        {
            var coreConfig = CoreConfigSingboxService.BuildMinimizedClientConfig(config, baseNode!);
            coreConfig.inbounds.Add(new Inbound4Sbox
            {
                type = nameof(EInboundProtocol.mixed),
                tag = InboundTag,
                listen = Global.Loopback,
                listen_port = GetMixedPort(config),
                users = users,
            });
            coreConfig.outbounds.AddRange(servers.OfType<Outbound4Sbox>());
            coreConfig.endpoints ??= [];
            coreConfig.endpoints.AddRange(servers.OfType<Endpoints4Sbox>());
            coreConfig.route.rules.AddRange(rules);
            coreConfig.route.rules.Add(new Rule4Sbox
            {
                inbound = [InboundTag],
                action = "reject",
            });

            result.ConfigJson = JsonUtils.Serialize(coreConfig, false);
            result.Success = result.ConfigJson.IsNotEmpty();
        }
        catch (Exception ex)
        {
            Logging.SaveLog(nameof(RotatingProxySingboxConfigService), ex);
            result.ErrorMessage = ex.Message;
        }
        return result;
    }

    private static int GetMixedPort(Config config)
    {
        var port = config.RotatingProxyItem?.MixedPort ?? 20808;
        return port is > 0 and <= 65535 ? port : 20808;
    }

    private static ERotatingProxySkipReason? GetSkipReason(ProfileItem node, HashSet<string> acceptedIndexIds)
    {
        if (node.IndexId.IsNullOrEmpty())
        {
            return ERotatingProxySkipReason.EmptyIndexId;
        }
        if (node.ConfigType == EConfigType.Custom)
        {
            return ERotatingProxySkipReason.Custom;
        }
        if (node.ConfigType.IsComplexType())
        {
            return ERotatingProxySkipReason.Complex;
        }
        if (!Global.SingboxSupportConfigType.Contains(node.ConfigType))
        {
            return ERotatingProxySkipReason.UnsupportedConfigType;
        }
        if (!node.IsValid()
            || !NodeValidator.Validate(node, ECoreType.sing_box).Success
            || !HasRequiredProtocolFields(node))
        {
            return ERotatingProxySkipReason.Invalid;
        }
        if (acceptedIndexIds.Contains(node.IndexId))
        {
            return ERotatingProxySkipReason.DuplicateIndexId;
        }
        return null;
    }

    private static bool HasRequiredProtocolFields(ProfileItem node)
    {
        if ((node.ConfigType is EConfigType.Trojan or EConfigType.Hysteria2 or EConfigType.Anytls)
            && node.Password.IsNullOrEmpty())
        {
            return false;
        }
        if ((node.ConfigType is EConfigType.TUIC or EConfigType.Naive)
            && (node.Username.IsNullOrEmpty() || node.Password.IsNullOrEmpty()))
        {
            return false;
        }
        if (node.ConfigType != EConfigType.WireGuard)
        {
            return true;
        }

        var protocolExtra = node.GetProtocolExtra();
        if (node.Password.IsNullOrEmpty() || protocolExtra.WgPublicKey.IsNullOrEmpty())
        {
            return false;
        }
        return (Utils.String2List(protocolExtra.WgReserved) ?? [])
            .All(t => byte.TryParse(t.TrimEx(), out _));
    }

    private static bool IsGeneratedServerValid(BaseServer4Sbox server, string expectedTag)
    {
        if (server.tag != expectedTag || server.type.IsNullOrEmpty())
        {
            return false;
        }
        if (server is Endpoints4Sbox endpoint)
        {
            return endpoint.private_key.IsNotEmpty()
                   && endpoint.address?.Count > 0
                   && endpoint.peers?.Count == 1
                   && endpoint.peers[0].address.IsNotEmpty()
                   && endpoint.peers[0].port is > 0 and <= 65535
                   && endpoint.peers[0].public_key.IsNotEmpty();
        }
        if (server is not Outbound4Sbox outbound)
        {
            return false;
        }
        if (outbound.type == Global.ProtocolTypes[EConfigType.Hysteria2] && outbound.realm is not null)
        {
            return true;
        }
        return outbound.server.IsNotEmpty()
               && (outbound.server_port is > 0 and <= 65535 || outbound.server_ports?.Count > 0);
    }

    private static RotatingProxyCandidate CreateCandidate(ProfileItem node,
        IReadOnlyDictionary<string, string>? subscriptionRemarks)
    {
        return new RotatingProxyCandidate
        {
            IndexId = node.IndexId ?? string.Empty,
            SubscriptionId = node.Subid ?? string.Empty,
            Remarks = node.Remarks ?? string.Empty,
            SubscriptionRemarks = subscriptionRemarks?.GetValueOrDefault(node.Subid ?? string.Empty) ?? string.Empty,
        };
    }

    private static RotatingProxySkippedNode CreateSkippedNode(RotatingProxyCandidate candidate,
        ERotatingProxySkipReason reason)
    {
        return new RotatingProxySkippedNode
        {
            IndexId = candidate.IndexId,
            SubscriptionId = candidate.SubscriptionId,
            Remarks = candidate.Remarks,
            SubscriptionRemarks = candidate.SubscriptionRemarks,
            Reason = reason,
        };
    }

    private static Dictionary<string, RotatingProxyCredential> CloneCredentialMap(
        IReadOnlyDictionary<string, RotatingProxyCredential>? credentialMap)
    {
        return credentialMap?.ToDictionary(
            item => item.Key,
            item => new RotatingProxyCredential
            {
                Username = item.Value.Username,
                Password = item.Value.Password,
            }) ?? [];
    }

    private static RotatingProxyCredential GetOrCreateCredential(string indexId,
        Dictionary<string, RotatingProxyCredential> credentialMap,
        HashSet<string> usedUsernames)
    {
        if (credentialMap.TryGetValue(indexId, out var credential)
            && credential.Username.IsNotEmpty()
            && credential.Password.IsNotEmpty()
            && usedUsernames.Add(credential.Username))
        {
            return credential;
        }

        string username;
        do
        {
            username = $"rp-{Convert.ToHexString(RandomNumberGenerator.GetBytes(10)).ToLowerInvariant()}";
        } while (!usedUsernames.Add(username));

        credential = new RotatingProxyCredential
        {
            Username = username,
            Password = Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant(),
        };
        credentialMap[indexId] = credential;
        return credential;
    }

    private static string CreateOutboundTag(string indexId, HashSet<string> usedOutboundTags)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(indexId));
        var baseTag = $"{OutboundTagPrefix}{Convert.ToHexString(hash.AsSpan(0, 8)).ToLowerInvariant()}";
        var tag = baseTag;
        var suffix = 1;
        while (!usedOutboundTags.Add(tag))
        {
            tag = $"{baseTag}-{suffix++}";
        }
        return tag;
    }
}
