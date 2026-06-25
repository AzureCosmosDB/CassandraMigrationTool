using Microsoft.AspNetCore.Components.Authorization;
using System.Security.Claims;

namespace CassandraMigrationWebApp.Service;
public class CustomAuthenticationStateProvider : AuthenticationStateProvider
{
    private readonly AuthenticationService _authService;

    public CustomAuthenticationStateProvider(AuthenticationService authService)
    {
        _authService = authService;
    }

    public override async Task<AuthenticationState> GetAuthenticationStateAsync()
    {
        var isAuthenticated = await _authService.IsAuthenticatedAsync();
        var username = isAuthenticated ? await _authService.GetCurrentUsernameAsync() : null;

        ClaimsIdentity identity = isAuthenticated
            ? new ClaimsIdentity(new[]
            {
                new Claim(ClaimTypes.Name, string.IsNullOrWhiteSpace(username) ? "Unknown" : username)
            }, "Custom Authentication")
            : new ClaimsIdentity();

        var user = new ClaimsPrincipal(identity);
        return new AuthenticationState(user);
    }
}
