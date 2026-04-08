using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Models
{

    public enum TaskResult
    {
        Success,
        Retry,
        Abort,
        FailedAfterRetries,
        Canceled,
        HasMore

    }
}
