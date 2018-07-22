using System;

using Quartz.Simpl;

namespace Quartz.Impl.RavenDB
{
    internal class FiredTrigger
    {
        public string Id { get; private set; }
        public string InstanceId { get; private set; }
        public string Scheduler { get; private set; }
        public string TriggerName { get; private set; }
        public string TriggerGroup { get; private set; }
        public string JobName { get; private set; }
        public string JobGroup { get; private set; }
        public DateTimeOffset FiredTime { get; private set; }
        public DateTimeOffset ScheduledTime { get; private set; }
        public int Priority { get; private set; }
        public InternalTriggerState State { get; private set; }
        public bool IsNonConcurrent { get; private set; }
        public bool RequestsRecovery { get; private set; }
    }
}