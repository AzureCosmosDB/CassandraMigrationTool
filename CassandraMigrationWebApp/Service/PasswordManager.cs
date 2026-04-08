using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Persistence;
using System.Security.Cryptography;
using System.Text;

namespace CassandraMigrationWebApp.Service
{
    public class PasswordManager
    {
        private const string PasswordFileName = "app.password";
        private const string KeyFileName = "app.keyfile";

        private readonly string _passwordFilePath;
        private readonly string _keyFilePath;
        private byte[]? _encryptionKey;

        public PasswordManager()
        {
            var workingFolder = Helper.GetWorkingFolder();
            
            if (!Directory.Exists(workingFolder))
            {
                Directory.CreateDirectory(workingFolder);
            }

            _passwordFilePath = Path.Combine(workingFolder, PasswordFileName);
            _keyFilePath = Path.Combine(workingFolder, KeyFileName);
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
            return password == storedPassword;
        }

        public Task<string?> GetStoredPasswordAsync()
        {
            if (!StorageStreamFactory.Exists(_passwordFilePath))
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
            return Task.FromResult(StorageStreamFactory.Exists(_passwordFilePath));
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
}
