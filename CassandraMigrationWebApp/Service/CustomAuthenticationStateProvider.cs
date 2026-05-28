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

        ClaimsIdentity identity;

        if (isAuthenticated)
        {
            identity = new ClaimsIdentity(new[]
            {
                new Claim(ClaimTypes.Name, "User")
            }, "Custom Authentication");
        }
        else
        {
            identity = new ClaimsIdentity();
        }

        var user = new ClaimsPrincipal(identity);
        return new AuthenticationState(user);
    }
}
