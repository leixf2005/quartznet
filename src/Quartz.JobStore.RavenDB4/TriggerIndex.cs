using System.Linq;

using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenDB
{
    internal class TriggerIndex : AbstractIndexCreationTask<Trigger>
    {
        public TriggerIndex()
        {
            Map = triggers => from trigger in triggers
                select new
                {
                    trigger.Name,
                    trigger.Group,
                    trigger.JobId,
                    trigger.MisfireInstruction,
                    trigger.NextFireTimeTicks,
                    trigger.NextFireTimeUtc,
                    trigger.Priority,
                    trigger.State,
                    trigger.Scheduler,
                    trigger.CalendarName
                };
        }
    }
}