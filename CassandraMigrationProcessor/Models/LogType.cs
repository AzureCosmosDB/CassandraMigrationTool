namespace CassandraMigrationProcessor
{
    /// <summary>
    /// Log type enumeration with severity levels (lower = more severe)
    /// </summary>
    public enum LogType
    {
        /// <summary>Error messages only</summary>
        Error = 0,
        
        /// <summary>
        /// [DEPRECATED] Use Info instead. Kept for backward compatibility with old log files.
        /// Will be removed in a future version.
        /// </summary>
        [Obsolete("Use Info instead. This value is kept only for backward compatibility with old log files.")]
        Message = 1,
        
        /// <summary>Warning messages</summary>
        Warning = 2,
        
        /// <summary>Informational messages (includes errors and warnings)</summary>
        Info = 3,
        
        /// <summary>Debug messages (includes errors, warnings, and info)</summary>
        Debug = 4,
        
        /// <summary>Verbose/detailed messages (includes all)</summary>
        Verbose = 5
    }
}