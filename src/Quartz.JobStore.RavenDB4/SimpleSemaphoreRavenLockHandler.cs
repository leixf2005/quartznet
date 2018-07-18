using System;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Impl.AdoJobStore;

namespace Quartz.Impl.RavenDB
{
    public class SimpleSemaphoreRavenLockHandler : IRavenLockHandler
    {
        private readonly SimpleSemaphore simpleSemaphore = new SimpleSemaphore();

        public Task<bool> ObtainLock(
            Guid requestorId, 
            RavenConnection connection, 
            string lockName,
            CancellationToken cancellationToken = default)
        {
            return simpleSemaphore.ObtainLock(requestorId, null, lockName, cancellationToken);
        }

        public Task ReleaseLock(
            Guid requestorId, 
            string lockName, 
            CancellationToken cancellationToken = default)
        {
            return simpleSemaphore.ReleaseLock(requestorId, lockName, cancellationToken);
        }
    }
}