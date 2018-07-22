using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;

namespace Quartz.Impl.RavenDB
{
    internal class RavenConnection : IDisposable
    {
        private IAsyncDocumentSession session;
        private readonly string schedulerName;
        private DateTimeOffset? sigChangeForTxCompletion;

        public RavenConnection(IAsyncDocumentSession session, string schedulerName)
        {
            this.session = session;
            this.schedulerName = schedulerName;
        }

        internal virtual DateTimeOffset? SignalSchedulingChangeOnTxCompletion
        {
            get => sigChangeForTxCompletion;
            set
            {
                DateTimeOffset? sigTime = sigChangeForTxCompletion;
                if (sigChangeForTxCompletion == null && value.HasValue)
                {
                    sigChangeForTxCompletion = value;
                }
                else
                {
                    if (sigChangeForTxCompletion == null || value < sigTime)
                    {
                        sigChangeForTxCompletion = value;
                    }
                }
            }
        }

        public Task Commit(CancellationToken cancellationToken)
        {
            return session.SaveChangesAsync(cancellationToken);
        }

        public void Rollback()
        {
            session?.Dispose();
            session = null;
        }

        public IRavenQueryable<Trigger> QueryTriggers()
        {
            return session.Query<Trigger, TriggerIndex>()
                .Where(x => x.Scheduler == schedulerName);
        }

        public IRavenQueryable<Job> QueryJobs()
        {
            return session.Query<Job, JobIndex>()
                .Where(x => x.Scheduler == schedulerName);
        }
        public IRavenQueryable<FiredTrigger> QueryFiredTriggers()
        {
            return session.Query<FiredTrigger, FiredTriggerIndex>()
                .Where(x => x.Scheduler == schedulerName);
        }

        public Task<Scheduler> LoadScheduler(CancellationToken cancellationToken)
        {
            return session.LoadAsync<Scheduler>(schedulerName, cancellationToken);
        }

        public Task<Trigger> LoadTrigger(TriggerKey triggerKey, CancellationToken cancellationToken)
        {
            return session.LoadAsync<Trigger>(triggerKey.DocumentId(schedulerName), cancellationToken);
        }

        public Task<Job> LoadJob(JobKey jobKey, CancellationToken cancellationToken)
        {
            return session.LoadAsync<Job>(jobKey.DocumentId(schedulerName), cancellationToken);
        }

        public Task<bool> ExistsAsync(string id)
        {
            return session.Advanced.ExistsAsync(id);
        }

        public Task StoreAsync(object entity, string id, CancellationToken cancellationToken)
        {
            return session.StoreAsync(entity, id, cancellationToken);
        }

        public void Delete(object entity)
        {
            session.Delete(entity);
        }

        public void Dispose()
        {
            session?.Dispose();
            session = null;
        }
    }
}