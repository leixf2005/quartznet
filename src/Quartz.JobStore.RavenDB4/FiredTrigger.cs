using System;

using Quartz.Simpl;

namespace Quartz.Impl.RavenDB
{
    internal class FiredTrigger
    {
        public string Id { get; private set; }
        public string Scheduler { get; private set; }
        public string TriggerId { get; private set; }
        public string JobId { get; private set; }
        public DateTimeOffset FiredTime { get; private set; }
        public DateTimeOffset ScheduledTime { get; private set; }
        public int Priority { get; private set; }
        public InternalTriggerState State { get; private set; }
        public bool IsNonConcurrent { get; private set; }
        public bool RequestsRecovery { get; private set; }

        public FiredTriggerRecord Deserialize()
        {
            var rec = new FiredTriggerRecord();
            rec.FireInstanceId = Id;
            rec.SchedulerInstanceName = Scheduler;
            rec.FireInstanceState = State;
            rec.FireTimestamp = FiredTime;
            rec.ScheduleTimestamp = ScheduledTime;
            rec.Priority = Priority;
            rec.TriggerKey = TriggerId.TriggerIdFromDocumentId();
            if (rec.FireInstanceState != InternalTriggerState.Acquired)
            {
                rec.JobDisallowsConcurrentExecution = IsNonConcurrent;
                rec.JobRequestsRecovery = RequestsRecovery;
                rec.JobKey = JobId.JobKeyFromDocumentId();
            }

            return rec;
        }
    }
}