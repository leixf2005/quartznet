using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;

namespace Quartz.Impl.RavenDB
{
    public class RavenConnection
    {
        private IAsyncDocumentSession session;
        private DateTimeOffset? sigChangeForTxCompletion;

        public RavenConnection(IAsyncDocumentSession session)
        {
            this.session = session;
        }

        public IAsyncDocumentSession Session => session;
        
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

        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return session.Query<T, TIndexCreator>();
        }

        public Task<T> LoadAsync<T>(string id, CancellationToken cancellationToken)
        {
            return session.LoadAsync<T>(id, cancellationToken);
        }
    }
}