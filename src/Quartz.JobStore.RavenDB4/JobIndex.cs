using System.Linq;

using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenDB
{
    internal class JobIndex : AbstractIndexCreationTask<Job>
    {
        public JobIndex()
        {
            Map = jobs => from job in jobs
                select new
                {
                    job.Key,
                    job.Group, 
                    job.RequestsRecovery, 
                    job.Scheduler
                };
        }
    }
}