using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Persistence;
using System.Security.Cryptography;
using System.Text;

namespace CassandraMigrationWebApp.Service;
public class PasswordManager
{
    /// <summary>
    /// File name used to persist the encrypted password payload in the application's working directory.
    /// </summary>
    /// <remarks>
    /// This file is intended to store encrypted credential data, not plaintext. It is persisted across application restarts.
    /// Access to this file should be restricted because it contains sensitive encrypted material.
    /// </remarks>
    private const string PasswordFileName = "app.password";
    /// <summary>
    /// File name used to persist the symmetric encryption key associated with <see cref="PasswordFileName"/>.
    /// </summary>
    /// <remarks>
    /// The key is stored on the same filesystem as the encrypted password file and is persisted across restarts.
    /// If an attacker can read both files, the encrypted credential data may be decrypted; therefore filesystem permissions
    /// and host-level protections are required.
    /// </remarks>
    private const string KeyFileName = "app.keyfile";

    private readonly string _passwordFilePath;
    private readonly string _keyFilePath;
    private readonly Microsoft.Extensions.Logging.ILogger<PasswordManager> _logger;
    private byte[]? _encryptionKey;

    public PasswordManager(Microsoft.Extensions.Logging.ILogger<PasswordManager> logger)
    {
        _logger = logger;
        var workingFolder = DataDirectoryResolver.GetWorkingFolder();

        if (!Directory.Exists(workingFolder))
        {
            Directory.CreateDirectory(workingFolder);
        }

        _passwordFilePath = Path.Join(workingFolder, PasswordFileName);
        _keyFilePath = Path.Join(workingFolder, KeyFileName);
    }

    /// <summary>
    /// Gets or creates the encryption key. Generated once per
    /// deployment and stored alongside the password file.
    /// </summary>
    private byte[] GetEncryptionKey()
    {
        if (_encryptionKey != null) return _encryptionKey;

        if (File.Exists(_keyFilePath))
        {
            _encryptionKey = File.ReadAllBytes(_keyFilePath);
            if (_encryptionKey.Length == 32)
                return _encryptionKey;

            _logger.LogWarning("PasswordManager: invalid keyfile detected, regenerating key. Previously stored passwords will be invalidated.");
        }

        // Generate a new random 256-bit key
        _encryptionKey = RandomNumberGenerator.GetBytes(32);
        var dir = Path.GetDirectoryName(_keyFilePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);
        File.WriteAllBytes(_keyFilePath, _encryptionKey);
        return _encryptionKey;
    }

    public async Task<bool> ValidatePasswordAsync(string password)
    {
        var storedPassword = await GetStoredPasswordAsync();
        if (storedPassword == null)
        {
            return false;
        }
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var storedPasswordBytes = Encoding.UTF8.GetBytes(storedPassword);
        try
        {
            return CryptographicOperations.FixedTimeEquals(passwordBytes, storedPasswordBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(passwordBytes);
            CryptographicOperations.ZeroMemory(storedPasswordBytes);
        }
    }

    public Task<string?> GetStoredPasswordAsync()
    {
        if (!FileSystem.Exists(_passwordFilePath))
        {
            return Task.FromResult<string?>(null);
        }

        try
        {
            byte[] encryptedBytes = File.ReadAllBytes(_passwordFilePath);

            var decryptedPassword = Decrypt(encryptedBytes);
            return Task.FromResult<string?>(decryptedPassword);
        }
        catch
        {
            // If decryption fails, return null
            return Task.FromResult<string?>(null);
        }
    }

    public Task<bool> IsPasswordSetAsync()
    {
        if (!FileSystem.Exists(_passwordFilePath))
            return Task.FromResult(false);

        // Verify the file is actually readable (key may have changed)
        try
        {
            byte[] encryptedBytes = File.ReadAllBytes(_passwordFilePath);
            Decrypt(encryptedBytes);
            return Task.FromResult(true);
        }
        catch
        {
            // Password file exists but is unreadable (key changed, container restarted).
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
    }

    public Task SetPasswordAsync(string newPassword)
    {
        var encryptedBytes = Encrypt(newPassword);

        // Ensure directory exists for local file
        var directory = Path.GetDirectoryName(_passwordFilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllBytes(_passwordFilePath, encryptedBytes);
        return Task.CompletedTask;
    }

    private byte[] Encrypt(string plainText)
    {
        using (Aes aes = Aes.Create())
        {
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = GetEncryptionKey();
            aes.GenerateIV();

            using (var encryptor = aes.CreateEncryptor(aes.Key, aes.IV))
            using (var ms = new MemoryStream())
            {
                // Write IV to the beginning of the stream
                ms.Write(aes.IV, 0, aes.IV.Length);

                using (var cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
                using (var sw = new StreamWriter(cs))
                {
                    sw.Write(plainText);
                }

                return ms.ToArray();
            }
        }
    }

    private string Decrypt(byte[] cipherText)
    {
        using (Aes aes = Aes.Create())
        {
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = GetEncryptionKey();

            // Extract IV from the beginning of the cipher text
            byte[] iv = new byte[aes.IV.Length];
            Array.Copy(cipherText, 0, iv, 0, iv.Length);
            aes.IV = iv;

            using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
            using (var ms = new MemoryStream(cipherText, iv.Length, cipherText.Length - iv.Length))
            using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
            using (var sr = new StreamReader(cs))
            {
                return sr.ReadToEnd();
            }
        }
    }
}
