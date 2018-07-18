using System.Linq;

using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenDB
{
    public class TriggerIndex : AbstractIndexCreationTask<Trigger>
    {
        public TriggerIndex()
        {
            Map = triggers => from trigger in triggers
                select new
                {
                    trigger.Key,
                    trigger.JobName,
                    trigger.Group,
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