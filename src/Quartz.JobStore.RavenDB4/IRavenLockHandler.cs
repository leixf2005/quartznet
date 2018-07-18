using System;
using System.Threading;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB
{
    internal interface IRavenLockHandler
    {
        Task<bool> ObtainLock(
            Guid requestorId,
            RavenConnection connection,
            string lockName,
            CancellationToken cancellationToken = default);

        Task ReleaseLock(
            Guid requestorId,
            string lockName,
            CancellationToken cancellationToken = default);
    }
}