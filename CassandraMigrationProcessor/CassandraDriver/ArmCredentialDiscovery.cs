using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace CassandraMigrationProcessor.CassandraDriver;
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

    private static readonly HttpClient _armHttpClient = new()
    {
        // A hung ARM endpoint must not stall job creation
        // indefinitely. 15s is generous for any control-plane GET.
        Timeout = TimeSpan.FromSeconds(15),
    };

    private const int ThrottleRetries = 3;

    /// <summary>Azure Instance Metadata Service — well-known endpoint (docs.microsoft.com/azure/virtual-machines/instance-metadata-service)</summary>
    private const string ImdsEndpoint = "http://169.254.169.254/metadata/instance";

    /// <summary>
    /// Sends an ARM request with strict response-code branching:
    /// returns the response on 2xx; logs a distinguishing line and
    /// returns null on 404 (genuinely "no such resource"); retries
    /// 429 (with Retry-After backoff) up to <see cref="ThrottleRetries"/>;
    /// throws on 401/403/5xx (operator-actionable: wrong RBAC role
    /// or genuine ARM outage). Caller disposes the response.
    /// </summary>
    private static async Task<HttpResponseMessage?> SendArmRequestAsync(
        Func<HttpRequestMessage> buildRequest, string context)
    {
        for (int attempt = 1; attempt <= ThrottleRetries; attempt++)
        {
            using var req = buildRequest();
            var resp = await _armHttpClient.SendAsync(req);

            if (resp.IsSuccessStatusCode)
                return resp;

            switch (resp.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    Console.WriteLine(
                        $"[INFO] ARM ({context}): 404 — no matching " +
                        $"resource in this subscription.");
                    resp.Dispose();
                    return null;

                case HttpStatusCode.Unauthorized:
                    resp.Dispose();
                    throw new InvalidOperationException(
                        $"ARM ({context}) returned 401 Unauthorized. " +
                        $"The current identity's token was rejected. " +
                        $"Re-acquire credentials and try again.");

                case HttpStatusCode.Forbidden:
                    resp.Dispose();
                    throw new InvalidOperationException(
                        $"ARM ({context}) returned 403 Forbidden. " +
                        $"The caller lacks the RBAC role required " +
                        $"(typically 'Cosmos DB Account Reader Role' " +
                        $"or 'DocumentDB Account Contributor').");

                case HttpStatusCode.TooManyRequests:
                    // Retry-After comes in two RFC 7231 §7.1.3 shapes
                    // that are mutually exclusive on the wire:
                    //   "Retry-After: 30"           -> Delta
                    //   "Retry-After: Wed, 21 Oct"  -> Date
                    // ARM normally uses Delta but is allowed to send
                    // Date; we previously silently fell through to
                    // 2*attempt seconds on the Date form and pounded
                    // a still-throttled endpoint.
                    var ra = resp.Headers.RetryAfter;
                    TimeSpan retryAfter = ra?.Delta
                        ?? (ra?.Date is { } d
                                ? d - DateTimeOffset.UtcNow
                                : (TimeSpan?)null)
                        ?? TimeSpan.FromSeconds(2 * attempt);
                    if (retryAfter < TimeSpan.Zero)
                        retryAfter = TimeSpan.FromSeconds(2 * attempt);
                    Console.WriteLine(
                        $"[WARN] ARM ({context}): 429 throttle — " +
                        $"sleeping {retryAfter.TotalSeconds:F1}s " +
                        $"(attempt {attempt}/{ThrottleRetries}).");
                    resp.Dispose();
                    if (attempt == ThrottleRetries) return null;
                    await Task.Delay(retryAfter);
                    continue;

                default:
                    var code = (int)resp.StatusCode;
                    resp.Dispose();
                    if (code >= 500)
                        throw new InvalidOperationException(
                            $"ARM ({context}) returned {code} " +
                            $"({resp.StatusCode}) — service outage. " +
                            $"Retry later.");
                    throw new InvalidOperationException(
                        $"ARM ({context}) returned {code} " +
                        $"({resp.StatusCode}).");
            }
        }
        return null;
    }

    /// <summary>
    /// Convenience overload: builds an authenticated GET/POST request
    /// against ARM (Bearer <paramref name="armToken"/>) and dispatches
    /// it through the retry/branching path above. Lets callers issue
    /// an ARM call in a single line rather than re-stating the
    /// request-build lambda each time.
    /// </summary>
    private static Task<HttpResponseMessage?> SendArmRequestAsync(
        HttpMethod method, string url, string armToken, string context)
        => SendArmRequestAsync(() =>
        {
            var r = new HttpRequestMessage(method, url);
            r.Headers.Authorization =
                new AuthenticationHeaderValue("Bearer", armToken);
            return r;
        }, context);

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
            subscriptionId = await GetSubscriptionIdFromInstanceMetadata();
        }

        if (string.IsNullOrEmpty(subscriptionId))
        {
            return new ArmCredentialResult();
        }

        // 1. Check MI clusters
        var miResult = await CheckManagedInstanceClusters(
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
    private static async Task<string?> GetSubscriptionIdFromInstanceMetadata()
    {
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get,
                ImdsEndpoint + "?api-version=2021-02-01");
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
            Console.WriteLine($"[WARN] GetSubscriptionIdFromInstanceMetadata failed: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Search MI clusters in the subscription for one whose
    /// seed nodes match the target contact point.
    /// </summary>
    private static async Task<ArmCredentialResult?> CheckManagedInstanceClusters(
        string armToken, string subscriptionId,
        string targetContactPoint)
    {
        try
        {
            var url = $"https://management.azure.com/subscriptions/" +
                $"{subscriptionId}/providers/Microsoft.DocumentDB/" +
                $"cassandraClusters?api-version=2024-05-15";

            using var resp = await SendArmRequestAsync(
                HttpMethod.Get, url, armToken, "list MI clusters");
            if (resp == null) return null;

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
        catch (InvalidOperationException)
        {
            // Operator-actionable 401/403/5xx from SendArmRequestAsync.
            // Re-throw so the operator sees the actual cause (RBAC
            // missing, token rejected, ARM outage) instead of the
            // ambiguous empty ArmCredentialResult that previously
            // looked indistinguishable from "no matching resource".
            // This neutralised a stated goal of PR #220's new throws.
            throw;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ARM discovery: {ex.Message}");
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

            using var listResp = await SendArmRequestAsync(
                HttpMethod.Get, listUrl, armToken, "list Cosmos accounts");
            if (listResp == null) return null;

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
                using var keysResp = await SendArmRequestAsync(
                    HttpMethod.Post, keysUrl, armToken,
                    $"list keys for {accountName}");

                if (keysResp != null)
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
        catch (InvalidOperationException)
        {
            // Operator-actionable 401/403/5xx from SendArmRequestAsync —
            // re-throw past the swallow-to-null catch so callers see
            // the real reason (RBAC role missing, token rejected, ARM
            // outage) instead of an ambiguous "no Cosmos account found".
            throw;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ARM discovery: {ex.Message}");
        }
        return null;
    }
}
