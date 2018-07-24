using System.Linq;

using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenDB
{
    internal class FiredTriggerIndex : AbstractIndexCreationTask<FiredTrigger>
    {
        public FiredTriggerIndex()
        {
            Map = triggers => from trigger in triggers
                select new
                {
                    trigger.Scheduler,
                    trigger.TriggerId,
                    trigger.JobId,
                    trigger.Priority,
                    trigger.State,
                };
        }
    }
}