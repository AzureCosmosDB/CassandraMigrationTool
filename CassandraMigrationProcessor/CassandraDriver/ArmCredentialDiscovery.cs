using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver
{
    /// <summary>
    /// ARM-based credential discovery for target Cassandra clusters.
    /// Discovers MI cluster auth methods and Cosmos DB account keys.
    /// </summary>
    public static class ArmCredentialDiscovery
    {
        /// <summary>
        /// Result of ARM-based credential discovery.
        /// </summary>
        internal class ArmCredentialResult
        {
            public string? AuthMethod { get; set; }
            public string? Username { get; set; }
            public string? Password { get; set; }
        }

        private static readonly HttpClient _armHttpClient = new();

        /// <summary>
        /// Discover target credentials via ARM control plane.
        /// Searches for Cassandra MI clusters and Cosmos DB accounts
        /// whose seed/contact points match the target IP/hostname.
        /// </summary>
        internal static async Task<ArmCredentialResult> DiscoverTargetCredentialsViaArm(
            string targetContactPoint, int targetPort)
        {
            var credential = new Azure.Identity.DefaultAzureCredential();
            var armToken = (await credential.GetTokenAsync(
                new Azure.Core.TokenRequestContext(
                    new[] { "https://management.azure.com/.default" }))).Token;

            // Discover subscription ID from environment or well-known
            string? subscriptionId = Environment.GetEnvironmentVariable(
                "AZURE_SUBSCRIPTION_ID");

            if (string.IsNullOrEmpty(subscriptionId))
            {
                // Try to discover from the current resource's metadata
                subscriptionId = await GetSubscriptionIdFromImds();
            }

            if (string.IsNullOrEmpty(subscriptionId))
            {
                return new ArmCredentialResult();
            }

            // 1. Check MI clusters
            var miResult = await CheckMiClusters(
                armToken, subscriptionId, targetContactPoint);
            if (miResult != null) return miResult;

            // 2. Check Cosmos DB Cassandra accounts
            var cosmosResult = await CheckCosmosAccounts(
                armToken, subscriptionId, targetContactPoint,
                targetPort);
            if (cosmosResult != null) return cosmosResult;

            return new ArmCredentialResult();
        }

        /// <summary>
        /// Try to get subscription ID from Azure IMDS
        /// (works in App Service and VMs).
        /// </summary>
        private static async Task<string?> GetSubscriptionIdFromImds()
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get,
                    "http://169.254.169.254/metadata/instance" +
                    "?api-version=2021-02-01");
                req.Headers.Add("Metadata", "true");
                using var resp = await _armHttpClient.SendAsync(req);
                if (!resp.IsSuccessStatusCode) return null;
                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                return doc.RootElement
                    .GetProperty("compute")
                    .GetProperty("subscriptionId")
                    .GetString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] GetSubscriptionIdFromImds failed: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Search MI clusters in the subscription for one whose
        /// seed nodes match the target contact point.
        /// </summary>
        private static async Task<ArmCredentialResult?> CheckMiClusters(
            string armToken, string subscriptionId,
            string targetContactPoint)
        {
            try
            {
                var url = $"https://management.azure.com/subscriptions/" +
                    $"{subscriptionId}/providers/Microsoft.DocumentDB/" +
                    $"cassandraClusters?api-version=2024-05-15";

                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Authorization =
                    new AuthenticationHeaderValue("Bearer", armToken);
                using var resp = await _armHttpClient.SendAsync(req);
                if (!resp.IsSuccessStatusCode) return null;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                foreach (var cluster in doc.RootElement
                    .GetProperty("value").EnumerateArray())
                {
                    var props = cluster.GetProperty("properties");

                    // Check seed nodes
                    if (props.TryGetProperty("seedNodes", out var seeds))
                    {
                        foreach (var seed in seeds.EnumerateArray())
                        {
                            var ip = seed.TryGetProperty(
                                "ipAddress", out var ipProp)
                                ? ipProp.GetString() : null;
                            if (ip == targetContactPoint)
                            {
                                var authMethod = props
                                    .TryGetProperty(
                                        "authenticationMethod",
                                        out var authProp)
                                    ? authProp.GetString() : null;

                                return new ArmCredentialResult
                                {
                                    AuthMethod = authMethod ?? "Unknown"
                                };
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                // ARM MI cluster search error — fall through
            }
            return null;
        }

        /// <summary>
        /// Search Cosmos DB accounts for a Cassandra API account
        /// matching the target contact point, and fetch its keys.
        /// </summary>
        private static async Task<ArmCredentialResult?> CheckCosmosAccounts(
            string armToken, string subscriptionId,
            string targetContactPoint, int targetPort)
        {
            // Only check Cosmos if port is 10350 (Cassandra API)
            if (targetPort != 10350) return null;

            try
            {
                // Derive account name from hostname
                // e.g. "myaccount.cassandra.cosmos.azure.com" → "myaccount"
                var hostParts = targetContactPoint.Split('.');
                if (hostParts.Length < 2) return null;
                var accountName = hostParts[0];

                // List Cosmos accounts to find matching one
                var listUrl = $"https://management.azure.com/subscriptions/" +
                    $"{subscriptionId}/providers/Microsoft.DocumentDB/" +
                    $"databaseAccounts?api-version=2024-05-15";

                using var listReq = new HttpRequestMessage(HttpMethod.Get, listUrl);
                listReq.Headers.Authorization =
                    new AuthenticationHeaderValue("Bearer", armToken);
                using var listResp = await _armHttpClient.SendAsync(listReq);
                if (!listResp.IsSuccessStatusCode) return null;

                var listJson = await listResp.Content.ReadAsStringAsync();
                using var listDoc = JsonDocument.Parse(listJson);

                foreach (var account in listDoc.RootElement
                    .GetProperty("value").EnumerateArray())
                {
                    var name = account.GetProperty("name").GetString();
                    if (!string.Equals(name, accountName,
                        StringComparison.OrdinalIgnoreCase))
                        continue;

                    // Found the account — fetch keys
                    var resourceId = account
                        .GetProperty("id").GetString();
                    var keysUrl = $"https://management.azure.com" +
                        $"{resourceId}/listKeys" +
                        $"?api-version=2024-05-15";
                    using var keysReq = new HttpRequestMessage(
                        HttpMethod.Post, keysUrl);
                    keysReq.Headers.Authorization =
                        new AuthenticationHeaderValue("Bearer", armToken);
                    using var keysResp = await _armHttpClient.SendAsync(keysReq);

                    if (keysResp.IsSuccessStatusCode)
                    {
                        var keysJson = await keysResp.Content
                            .ReadAsStringAsync();
                        using var keysDoc = JsonDocument.Parse(keysJson);
                        var primaryKey = keysDoc.RootElement
                            .TryGetProperty("primaryMasterKey",
                                out var keyProp)
                            ? keyProp.GetString() : null;

                        if (!string.IsNullOrEmpty(primaryKey))
                        {
                            return new ArmCredentialResult
                            {
                                AuthMethod = "Cassandra",
                                Username = accountName,
                                Password = primaryKey
                            };
                        }
                    }
                }
            }
            catch (Exception)
            {
                // ARM Cosmos account search error — fall through
            }
            return null;
        }
    }
}
