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
                    trigger.TriggerName,
                    trigger.TriggerGroup,
                    trigger.JobName,
                    trigger.JobGroup,
                    trigger.Priority,
                    trigger.State,
                };
        }
    }
}