using Microsoft.AspNetCore.Components.Server.ProtectedBrowserStorage;

namespace CassandraMigrationWebApp.Service
{
    public class AuthenticationService
    {
        private readonly ProtectedSessionStorage _sessionStorage;
        private readonly PasswordManager _passwordManager;
        private const string AuthTokenKey = "authToken";
        private const string AuthExpiryKey = "authExpiry";

        public AuthenticationService(ProtectedSessionStorage sessionStorage, PasswordManager passwordManager)
        {
            _sessionStorage = sessionStorage;
            _passwordManager = passwordManager;
        }

        public async Task<bool> ValidatePasswordAsync(string password)
        {
            var isValid = await _passwordManager.ValidatePasswordAsync(password);

            if (isValid)
            {
                var expiryTime = DateTime.UtcNow.AddHours(12);
                await _sessionStorage.SetAsync(AuthTokenKey, Guid.NewGuid().ToString());
                await _sessionStorage.SetAsync(AuthExpiryKey, expiryTime);
                return true;
            }
            return false;
        }

        public async Task<bool> IsAuthenticatedAsync()
        {
            try
            {
                var tokenResult = await _sessionStorage.GetAsync<string>(AuthTokenKey);
                var expiryResult = await _sessionStorage.GetAsync<DateTime>(AuthExpiryKey);

                if (!tokenResult.Success || !expiryResult.Success)
                {
                    return false;
                }

                if (string.IsNullOrEmpty(tokenResult.Value))
                {
                    return false;
                }

                if (expiryResult.Value < DateTime.UtcNow)
                {
                    await LogoutAsync();
                    return false;
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        public async Task LogoutAsync()
        {
            await _sessionStorage.DeleteAsync(AuthTokenKey);
            await _sessionStorage.DeleteAsync(AuthExpiryKey);
        }

        public async Task<DateTime?> GetExpiryTimeAsync()
        {
            try
            {
                var expiryResult = await _sessionStorage.GetAsync<DateTime>(AuthExpiryKey);
                return expiryResult.Success ? expiryResult.Value : null;
            }
            catch
            {
                return null;
            }
        }
    }
}
