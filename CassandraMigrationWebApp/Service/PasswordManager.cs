using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Persistence;
using System.Security.Cryptography;

namespace CassandraMigrationWebApp.Service;
public class PasswordManager
{
    /// <summary>
    /// File name used to persist the password hash in the application's working directory.
    /// </summary>
    /// <remarks>
    /// This file stores a PBKDF2 hash of the password, not plaintext. It is persisted across application restarts.
    /// Access to this file should be restricted because it contains sensitive hashed credential data.
    /// </remarks>
    private const string PasswordFileName = "app.password";
    private readonly string _passwordFilePath;
    private readonly ILogger<PasswordManager> _logger;

    public PasswordManager(ILogger<PasswordManager> logger)
    {
        _logger = logger;
        var workingFolder = DataDirectoryResolver.GetWorkingFolder();

        if (!Directory.Exists(workingFolder))
        {
            Directory.CreateDirectory(workingFolder);
        }

        _passwordFilePath = Path.Join(workingFolder, PasswordFileName);
    }

    private const int PasswordSaltSize = 16;
    private const int PasswordHashSize = 32;
    private const int PasswordIterations = 600_000;

    public async Task<bool> ValidatePasswordAsync(string password)
    {
        var storedPasswordHash = await GetStoredPasswordAsync();
        if (string.IsNullOrWhiteSpace(storedPasswordHash))
        {
            return false;
        }

        return VerifyPassword(password, storedPasswordHash);
    }

    public Task<string?> GetStoredPasswordAsync()
    {
        if (!FileSystem.Exists(_passwordFilePath))
        {
            return Task.FromResult<string?>(null);
        }

        try
        {
            var storedHash = File.ReadAllText(_passwordFilePath);
            return Task.FromResult<string?>(storedHash);
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "I/O error while reading stored password.");
            return Task.FromResult<string?>(null);
        }
        catch (UnauthorizedAccessException ex)
        {
            _logger.LogDebug(ex, "Access denied while reading stored password.");
            return Task.FromResult<string?>(null);
        }
    }

    public Task<bool> IsPasswordSetAsync()
    {
        if (!FileSystem.Exists(_passwordFilePath))
            return Task.FromResult(false);

        // Verify the file contains a valid password hash
        try
        {
            var hash = File.ReadAllText(_passwordFilePath);
            var parts = hash.Split(':');
            if (parts.Length == 3 && int.TryParse(parts[0], out var iterations) && iterations > 0)
                return Task.FromResult(true);

            // Hash format is invalid (e.g. corrupted or from an old format).
            // Delete the corrupt file so the user can set a new password. The delete
            // itself can fail if the file is held open by another process — surface
            // that to stderr so it shows up in container logs rather than vanishing.
            try
            {
                File.Delete(_passwordFilePath);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"PasswordManager: failed to delete corrupt password file '{_passwordFilePath}': {ex.GetType().Name}: {ex.Message}");
            }
            return Task.FromResult(false);
        }
        catch (IOException)
        {
            // File exists but could not be read due to I/O issues; do not treat as corruption.
            return Task.FromResult(false);
        }
        catch (UnauthorizedAccessException)
        {
            // File exists but access is denied; do not treat as corruption.
            return Task.FromResult(false);
        }
    }

    public Task SetPasswordAsync(string newPassword)
    {
        var passwordHash = HashPassword(newPassword);

        // Ensure directory exists for local file
        var directory = Path.GetDirectoryName(_passwordFilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(_passwordFilePath, passwordHash);

        // On Unix-like systems, explicitly restrict access to owner read/write (0600).
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            try
            {
                File.SetUnixFileMode(
                    _passwordFilePath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
            catch (PlatformNotSupportedException ex)
            {
                _logger.LogDebug(ex, "Unix file permissions are not supported on this platform.");
            }
            catch (UnauthorizedAccessException ex)
            {
                _logger.LogDebug(ex, "Access denied while setting restrictive permissions on password file.");
            }
            catch (IOException ex)
            {
                _logger.LogDebug(ex, "I/O error while setting restrictive permissions on password file.");
            }
        }

        return Task.CompletedTask;
    }

    private static string HashPassword(string password)
    {
        var salt = RandomNumberGenerator.GetBytes(PasswordSaltSize);
        var hash = Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            PasswordIterations,
            HashAlgorithmName.SHA256,
            PasswordHashSize);

        return $"{PasswordIterations}:{Convert.ToBase64String(salt)}:{Convert.ToBase64String(hash)}";
    }

    private static bool VerifyPassword(string password, string storedPasswordHash)
    {
        var parts = storedPasswordHash.Split(':');
        if (parts.Length != 3 || !int.TryParse(parts[0], out var iterations) || iterations <= 0)
        {
            return false;
        }

        byte[] salt;
        byte[] expectedHash;
        try
        {
            salt = Convert.FromBase64String(parts[1]);
            expectedHash = Convert.FromBase64String(parts[2]);
        }
        catch (FormatException)
        {
            return false;
        }

        var actualHash = Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            expectedHash.Length);

        try
        {
            return CryptographicOperations.FixedTimeEquals(actualHash, expectedHash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(actualHash);
            CryptographicOperations.ZeroMemory(expectedHash);
            CryptographicOperations.ZeroMemory(salt);
        }
    }

}
