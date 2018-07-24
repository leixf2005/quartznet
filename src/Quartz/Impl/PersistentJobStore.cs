using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Impl.Matchers;
using Quartz.Logging;
using Quartz.Simpl;
using Quartz.Spi;
using Quartz.Util;

namespace Quartz.Impl
{
    /// <summary>
    /// Base class that tries to be persistence technology agnostic and offers some common boiler plate
    /// code needed to hook job store properly.
    /// </summary>
    public abstract class PersistentJobStore<TUnitOfWorkConnection> : IJobStore where TUnitOfWorkConnection : UnitOfWorkConnection
    {
        private static long firedTriggerCounter = SystemTime.UtcNow().Ticks;

        private ClusterManager clusterManager;
        private MisfireHandler misfireHandler;

        private TimeSpan misfireThreshold = TimeSpan.FromMinutes(1); // one minute
        private TimeSpan? misfireHandlerFrequency;
        private string instanceName;

        protected PersistentJobStore()
        {
            Log = LogProvider.GetLogger(GetType());
        }
        
        /// <summary>
        /// Gets the log.
        /// </summary>
        /// <value>The log.</value>
        internal ILog Log { get; }

        protected ITypeLoadHelper TypeLoadHelper { get; set; }
        protected ISchedulerSignaler SchedulerSignaler { get; set; }

        /// <summary>
        /// Whether or not to obtain locks when inserting new jobs/triggers.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Defaults to <see langword="true" />, which is safest - some db's (such as
        /// MS SQLServer) seem to require this to avoid deadlocks under high load,
        /// while others seem to do fine without.  Settings this to false means
        /// isolation guarantees between job scheduling and trigger acquisition are
        /// entirely enforced by the database.  Depending on the database and it's
        /// configuration this may cause unusual scheduling behaviors.
        /// </para>
        /// <para>
        /// Setting this property to <see langword="false" /> will provide a
        /// significant performance increase during the addition of new jobs
        /// and triggers.
        /// </para>
        /// </remarks>
        public bool LockOnInsert { get; set; } = true;

        protected bool SchedulerRunning { get; private set; }
        protected bool IsShutdown { get; private set; }
        
        /// <summary>
        /// Gets or sets the number of retries before an error is logged for recovery operations.
        /// </summary>
        public int RetryableActionErrorLogThreshold { get; set; } = 4;

        /// <inheritdoc />
        public string InstanceId { get; set; }

        /// <inheritdoc />
        public string InstanceName
        {
            get => instanceName;
            set
            {
                ValidateInstanceName(value);
                instanceName = value;
            }
        }

        protected virtual void ValidateInstanceName(string value)
        {
        }

        /// <inheritdoc />
        public virtual int ThreadPoolSize
        {
            set { }
        }

        /// <summary>
        /// Get whether the threads spawned by this JobStore should be
        /// marked as daemon. Possible threads include the <see cref="MisfireHandler" />
        /// and the <see cref="ClusterManager"/>.
        /// </summary>
        /// <returns></returns>
        public bool MakeThreadsDaemons { get; set; }
        
        /// <inheritdoc />
        public bool SupportsPersistence => true;

        /// <inheritdoc />
        public abstract long EstimatedTimeToReleaseAndAcquireTrigger { get; }

        /// <inheritdoc />
        public bool Clustered { get; set; }

        /// <summary>
        /// Get or set the frequency at which this instance "checks-in"
        /// with the other instances of the cluster. -- Affects the rate of
        /// detecting failed instances.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan ClusterCheckinInterval { get; set; } = TimeSpan.FromMilliseconds(7500);

        /// <summary>
        /// Gets or sets the retry interval.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(15);

        /// <summary>
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireThreshold
        {
            get => misfireThreshold;
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }

        /// <summary>
        /// How often should the misfire handler check for misfires. Defaults to
        /// <see cref="MisfireThreshold"/>.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireHandlerFrequency
        {
            get => misfireHandlerFrequency.GetValueOrDefault(MisfireThreshold);
            // ReSharper disable once UnusedMember.Global
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireHandlerFrequency must be larger than 0");
                }
                misfireHandlerFrequency = value;
            }
        }
        
        /// <summary>
        /// Get whether to check to see if there are Triggers that have misfired
        /// before actually acquiring the lock to recover them.  This should be
        /// set to false if the majority of the time, there are misfired
        /// Triggers.
        /// </summary>
        /// <returns></returns>
        public bool DoubleCheckLockMisfireHandler { get; set; } = true;

        /// <summary>
        /// Get or set the maximum number of misfired triggers that the misfire handling
        /// thread will try to recover at one time (within one transaction).  The
        /// default is 20.
        /// </summary>
        public int MaxMisfiresToHandleAtATime { get; set; } = 20;

        protected virtual DateTimeOffset MisfireTime
        {
            get
            {
                DateTimeOffset misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1*MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        protected abstract IClusterManagementOperations ClusterManagementOperations { get; }

        protected abstract IMisfireHandlerOperations MisfireHandlerOperations { get; }
        
        /// <inheritdoc />
        public virtual Task Initialize(
            ITypeLoadHelper typeLoadHelper,
            ISchedulerSignaler schedulerSignaler,
            CancellationToken cancellationToken = default)
        {
            TypeLoadHelper = typeLoadHelper;
            SchedulerSignaler = schedulerSignaler;

            return TaskUtil.CompletedTask;
        }

        /// <inheritdoc />
        public virtual async Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            if (Clustered)
            {
                clusterManager = new ClusterManager(
                    InstanceId,
                    InstanceName,
                    MakeThreadsDaemons,
                    RetryInterval,
                    RetryableActionErrorLogThreshold,
                    SchedulerSignaler,
                    ClusterManagementOperations);

                await clusterManager.Initialize().ConfigureAwait(false);
            }
            else
            {
                try
                {
                    await RecoverJobs(cancellationToken).ConfigureAwait(false);
                }
                catch (SchedulerException se)
                {
                    Log.ErrorException("Failure occurred during job recovery: " + se.Message, se);
                    throw new SchedulerConfigException("Failure occurred during job recovery.", se);
                }
            }

            misfireHandler = new MisfireHandler(
                InstanceId,
                InstanceName,
                MakeThreadsDaemons,
                RetryInterval,
                RetryableActionErrorLogThreshold,
                SchedulerSignaler,
                MisfireHandlerOperations);

            misfireHandler.Initialize();

            SchedulerRunning = true;
        }
        
        /// <inheritdoc />
        public virtual Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            SchedulerRunning = false;
            return TaskUtil.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            SchedulerRunning = true;
            return TaskUtil.CompletedTask;
        }

        /// <inheritdoc />
        public virtual async Task Shutdown(CancellationToken cancellationToken = default)
        {
            IsShutdown = true;

            if (misfireHandler != null)
            {
                await misfireHandler.Shutdown().ConfigureAwait(false);
            }

            if (clusterManager != null)
            {
                await clusterManager.Shutdown().ConfigureAwait(false);
            }
        }
        
        protected string GetFiredTriggerRecordId()
        {
            Interlocked.Increment(ref firedTriggerCounter);
            return InstanceId + firedTriggerCounter;
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as appropriate.
        /// </summary>
        protected abstract Task RecoverJobs(CancellationToken cancellationToken = default);

        public virtual async Task<RecoverMisfiredJobsResult> RecoverMisfiredJobs(
            TUnitOfWorkConnection conn,
            bool recovering,
            CancellationToken cancellationToken = default)
        {
            // If recovering, we want to handle all of the misfired
            // triggers right away.
            int maxMisfiresToHandleAtATime = recovering ? -1 : MaxMisfiresToHandleAtATime;

            List<TriggerKey> misfiredTriggers = new List<TriggerKey>();
            DateTimeOffset earliestNewTime = DateTimeOffset.MaxValue;

            // We must still look for the MISFIRED state in case triggers were left
            // in this state when upgrading to this version that does not support it.
            bool hasMoreMisfiredTriggers = await GetMisfiredTriggersInWaitingState(
                conn, 
                maxMisfiresToHandleAtATime, 
                misfiredTriggers, 
                cancellationToken).ConfigureAwait(false);

            if (hasMoreMisfiredTriggers)
            {
                Log.Info($"Handling the first {misfiredTriggers.Count} triggers that missed their scheduled fire-time.  More misfired triggers remain to be processed.");
            }
            else if (misfiredTriggers.Count > 0)
            {
                Log.Info($"Handling {misfiredTriggers.Count} trigger(s) that missed their scheduled fire-time.");
            }
            else
            {
                Log.Debug("Found 0 triggers that missed their scheduled fire-time.");
                return RecoverMisfiredJobsResult.NoOp;
            }

            foreach (TriggerKey triggerKey in misfiredTriggers)
            {
                IOperableTrigger trigger = await RetrieveTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false);

                if (trigger == null)
                {
                    continue;
                }

                await DoUpdateOfMisfiredTrigger(
                    conn, 
                    trigger, 
                    forceState: false, 
                    InternalTriggerState.Waiting, 
                    recovering, 
                    cancellationToken).ConfigureAwait(false);

                DateTimeOffset? nextTime = trigger.GetNextFireTimeUtc();
                if (nextTime.HasValue && nextTime.Value < earliestNewTime)
                {
                    earliestNewTime = nextTime.Value;
                }
            }

            return new RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.Count, earliestNewTime);
        }

        protected abstract Task<bool> GetMisfiredTriggersInWaitingState(TUnitOfWorkConnection conn,
            int count,
            List<TriggerKey> resultList,
            CancellationToken cancellationToken);

        protected virtual async Task<bool> UpdateMisfiredTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            InternalTriggerState newStateIfNotComplete,
            bool forceState,
            CancellationToken cancellationToken = default)
        {
            try
            {
                IOperableTrigger trigger = await RetrieveTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false);

                DateTimeOffset misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1*MisfireThreshold.TotalMilliseconds);
                }

                if (trigger.GetNextFireTimeUtc().GetValueOrDefault() > misfireTime)
                {
                    return false;
                }

                await DoUpdateOfMisfiredTrigger(
                    conn, 
                    trigger, 
                    forceState, 
                    newStateIfNotComplete, 
                    recovering: false, 
                    cancellationToken).ConfigureAwait(false);

                return true;
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't update misfired trigger '{triggerKey}': {e.Message}", e);
            }
        }

        private async Task DoUpdateOfMisfiredTrigger(
            TUnitOfWorkConnection conn,
            IOperableTrigger trig,
            bool forceState,
            InternalTriggerState newStateIfNotComplete, 
            bool recovering,
            CancellationToken cancellationToken)
        {
            ICalendar cal = null;
            if (trig.CalendarName != null)
            {
                cal = await RetrieveCalendar(conn, trig.CalendarName, cancellationToken).ConfigureAwait(false);
            }

            await SchedulerSignaler.NotifyTriggerListenersMisfired(trig, cancellationToken).ConfigureAwait(false);

            trig.UpdateAfterMisfire(cal);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await StoreTrigger(conn, trig, null, true, InternalTriggerState.Complete, forceState, recovering, cancellationToken).ConfigureAwait(false);
                await SchedulerSignaler.NotifySchedulerListenersFinalized(trig, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await StoreTrigger(conn, trig, null, true, newStateIfNotComplete, forceState, recovering, cancellationToken).ConfigureAwait(false);
            }
        }
        
        /// <inheritdoc />
        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            await ExecuteInLock(LockOnInsert ? LockType.TriggerAccess : LockType.None, async conn =>
            {
                await StoreJob(conn, newJob, false, cancellationToken).ConfigureAwait(false);
                await StoreTrigger(conn, newTrigger, newJob, false, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockOnInsert || replaceExisting ? LockType.TriggerAccess : LockType.None,
                conn => StoreJob(conn, newJob, replaceExisting, cancellationToken),
                cancellationToken);
        }

        protected abstract Task StoreJob(
            TUnitOfWorkConnection conn, 
            IJobDetail jobDetail, 
            bool replaceExisting,
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
            bool replace, 
            CancellationToken cancellationToken = default)
        {
            await ExecuteInLock(
                LockOnInsert || replace ? LockType.TriggerAccess : LockType.None, async conn =>
                {
                    // TODO: make this more efficient with a true bulk operation...
                    foreach (var pair in triggersAndJobs)
                    {
                        var job = pair.Key;
                        var triggers = pair.Value;
                        await StoreJob(conn, job, replace, cancellationToken).ConfigureAwait(false);
                        foreach (var trigger in triggers)
                        {
                            await StoreTrigger(conn, (IOperableTrigger) trigger, job, replace, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }, cancellationToken).ConfigureAwait(false);
        }
        
        /// <inheritdoc />
        public Task TriggeredJobComplete(
            IOperableTrigger trigger,
            IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken = default)
        {
            return RetryExecuteInNonManagedTXLock(
                LockType.TriggerAccess, async conn =>
                {
                    try
                    {
                        if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                        {
                            if (!trigger.GetNextFireTimeUtc().HasValue)
                            {
                                // double check for possible reschedule within job
                                // execution, which would cancel the need to delete...
                                TriggerStatus stat = await GetTriggerStatus(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                                if (stat != null && !stat.NextFireTimeUtc.HasValue)
                                {
                                    await RemoveTrigger(conn, trigger.Key, jobDetail, cancellationToken).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                await RemoveTrigger(conn, trigger.Key, jobDetail, cancellationToken).ConfigureAwait(false);
                                conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                            }
                        }
                        else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                        {
                            await UpdateTriggerState(conn, trigger.Key, InternalTriggerState.Complete, cancellationToken).ConfigureAwait(false);
                            conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                        }
                        else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                        {
                            Log.Info("Trigger " + trigger.Key + " set to ERROR state.");
                            await UpdateTriggerState(conn, trigger.Key, InternalTriggerState.Error, cancellationToken).ConfigureAwait(false);
                            conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                        }
                        else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                        {
                            await UpdateTriggerStatesForJob(conn, trigger.JobKey, InternalTriggerState.Complete, cancellationToken).ConfigureAwait(false);
                            conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                        }
                        else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                        {
                            Log.Info("All triggers of Job " + trigger.JobKey + " set to ERROR state.");
                            await UpdateTriggerStatesForJob(conn, trigger.JobKey, InternalTriggerState.Error, cancellationToken).ConfigureAwait(false);
                            conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                        }

                        if (jobDetail.ConcurrentExecutionDisallowed)
                        {
                            await UpdateTriggerStatesForJobFromOtherState(conn, jobDetail.Key, InternalTriggerState.Waiting, InternalTriggerState.Blocked, cancellationToken).ConfigureAwait(false);
                            await UpdateTriggerStatesForJobFromOtherState(conn, jobDetail.Key, InternalTriggerState.Paused, InternalTriggerState.PausedAndBlocked, cancellationToken).ConfigureAwait(false);
                            conn.SignalSchedulingChangeOnTxCompletion = SchedulerConstants.SchedulingSignalDateTime;
                        }

                        if (jobDetail.PersistJobDataAfterExecution)
                        {
                            try
                            {
                                if (jobDetail.JobDataMap.Dirty)
                                {
                                    await UpdateJobData(conn, jobDetail, cancellationToken).ConfigureAwait(false);
                                }
                            }
                            catch (IOException e)
                            {
                                throw new JobPersistenceException("Couldn't serialize job data: " + e.Message, e);
                            }
                            catch (Exception e)
                            {
                                throw new JobPersistenceException("Couldn't update job data: " + e.Message, e);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("Couldn't update trigger state(s): " + e.Message, e);
                    }

                    try
                    {
                        await DeleteFiredTrigger(conn, trigger.FireInstanceId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("Couldn't delete fired trigger: " + e.Message, e);
                    }
                },
                cancellationToken);
        }

        protected abstract Task UpdateTriggerStatesForJobFromOtherState(
            TUnitOfWorkConnection conn, 
            JobKey jobKey, 
            InternalTriggerState newState, 
            InternalTriggerState oldState, 
            CancellationToken cancellationToken);

        protected abstract Task UpdateJobData(
            TUnitOfWorkConnection conn, 
            IJobDetail jobDetail, 
            CancellationToken cancellationToken);

        protected abstract Task DeleteFiredTrigger(
            TUnitOfWorkConnection conn, 
            string triggerFireInstanceId,
            CancellationToken cancellationToken);

        protected abstract Task UpdateTriggerStatesForJob(
            TUnitOfWorkConnection conn, 
            JobKey triggerJobKey, 
            InternalTriggerState state, 
            CancellationToken cancellationToken);

        protected abstract Task UpdateTriggerState(
            TUnitOfWorkConnection conn, 
            TriggerKey triggerKey, 
            InternalTriggerState state, 
            CancellationToken cancellationToken);

        protected abstract Task RetryExecuteInNonManagedTXLock(
            LockType triggerAccess, 
            Func<TUnitOfWorkConnection, Task> func, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteInLock(
                    LockType.TriggerAccess, 
                    conn => RemoveCalendar(conn, calName, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't remove calendar: {e.Message}", e);
            }
        }

        protected abstract Task<bool> RemoveCalendar(
            TUnitOfWorkConnection conn,
            string calendarName,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<ICalendar> RetrieveCalendar(
            string calendarName,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => RetrieveCalendar(conn, calendarName, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (IOException e)
            {
                throw new JobPersistenceException($"Couldn't retrieve calendar because it couldn't be deserialized: {e.Message}", e);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't retrieve calendar: {e.Message}", e);
            }
        }
        
        protected abstract Task<ICalendar> RetrieveCalendar(
            TUnitOfWorkConnection conn,
            string calendarName, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetNumberOfJobs(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't obtain number of jobs: " + e.Message, e);
            }
        }

        protected abstract Task<int> GetNumberOfJobs(
            TUnitOfWorkConnection conn,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetNumberOfTriggers(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't obtain number of triggers: " + e.Message, e);
            }
        }
        
        protected abstract Task<int> GetNumberOfTriggers(
            TUnitOfWorkConnection conn,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetNumberOfCalendars(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't obtain number of calendars: " + e.Message, e);
            }
        }

        protected abstract Task<int> GetNumberOfCalendars(
            TUnitOfWorkConnection conn,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(
            GroupMatcher<JobKey> matcher, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetJobKeys(conn, matcher, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't obtain job names: " + e.Message, e);
            }
        }

        protected abstract Task<IReadOnlyCollection<JobKey>> GetJobKeys(
            TUnitOfWorkConnection conn,
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default);

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetTriggerKeys(conn, matcher, cancellationToken), 
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't obtain trigger names: {e.Message}", e);
            }        
        }

        protected abstract Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
            TUnitOfWorkConnection conn, 
            GroupMatcher<TriggerKey> groupMatcher, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetJobGroupNames(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't obtain job groups: {e.Message}", e);
            }    
        }

        protected abstract Task<IReadOnlyCollection<string>> GetJobGroupNames(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetTriggerGroupNames(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't obtain trigger groups: {e.Message}", e);
            }        
        }

        protected abstract Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetCalendarNames(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't obtain calendar names: {e.Message}", e);
            }
        }

        protected abstract Task<IReadOnlyCollection<string>> GetCalendarNames(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
            JobKey jobKey, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetTriggersForJob(conn, jobKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't obtain triggers for job: {e.Message}", e);
            }
        }

        protected abstract Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
            TUnitOfWorkConnection conn, 
            JobKey jobKey, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<TriggerState> GetTriggerState(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetTriggerState(conn, triggerKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't determine state of trigger ({triggerKey}): {e.Message}", e);
            }
        }

        protected abstract Task<TriggerState> GetTriggerState(TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken);
                
        public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            try
            {
                await ExecuteInLock(
                    LockType.TriggerAccess,
                    conn => PauseTrigger(conn, triggerKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't pause trigger '{triggerKey}': {e.Message}", e);
            }        
        }

        protected abstract Task PauseTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken);

        public Task<IReadOnlyCollection<string>> PauseTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess, 
                conn => PauseTriggerGroup(conn, matcher, cancellationToken),
                cancellationToken);
        }

        protected abstract Task<IReadOnlyCollection<string>> PauseTriggerGroup(
            TUnitOfWorkConnection conn,
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                var triggers = await GetTriggersForJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                foreach (IOperableTrigger trigger in triggers)
                {
                    await PauseTrigger(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                }
            }, cancellationToken);
        }

        /// <inheritdoc />
        public Task<IReadOnlyCollection<string>> PauseJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock<IReadOnlyCollection<string>>(LockType.TriggerAccess, async conn =>
            {
                var groupNames = new ReadOnlyCompatibleHashSet<string>();
                var jobNames = await GetJobKeys(conn, matcher, cancellationToken).ConfigureAwait(false);

                foreach (JobKey jobKey in jobNames)
                {
                    var triggers = await GetTriggersForJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        await PauseTrigger(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                    }
                    groupNames.Add(jobKey.Group);
                }

                return groupNames;
            }, cancellationToken);
        }

        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            try
            {
                await ExecuteInLock(
                    LockType.TriggerAccess, 
                    conn => ResumeTrigger(conn, triggerKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't resume trigger '{triggerKey}': {e.Message}", e);
            }
        }

        protected async Task ResumeTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            TriggerStatus status = await GetTriggerStatus(conn, triggerKey, cancellationToken).ConfigureAwait(false);

            if (status?.NextFireTimeUtc == null || status.NextFireTimeUtc == DateTimeOffset.MinValue)
            {
                return;
            }

            bool blocked = status.Status == InternalTriggerState.PausedAndBlocked;

            var newState = await CheckBlockedState(conn, status.JobKey, InternalTriggerState.Waiting, cancellationToken).ConfigureAwait(false);

            bool misfired = false;

            if (SchedulerRunning && status.NextFireTimeUtc.Value < SystemTime.UtcNow())
            {
                misfired = await UpdateMisfiredTrigger(conn, triggerKey, newState, true, cancellationToken).ConfigureAwait(false);
            }

            if (!misfired)
            {
                var oldStates = blocked
                    ? new[] {InternalTriggerState.PausedAndBlocked}
                    : new[] {InternalTriggerState.Paused};
                
                await UpdateTriggerStateFromOtherStates(conn, triggerKey, newState, oldStates, cancellationToken).ConfigureAwait(false);
            }
        }
        
        protected abstract Task<TriggerStatus> GetTriggerStatus(
            TUnitOfWorkConnection conn, 
            TriggerKey triggerKey,
            CancellationToken cancellationToken);

        protected abstract Task<int> UpdateTriggerStateFromOtherStates(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            InternalTriggerState newState,
            InternalTriggerState[] oldStates,
            CancellationToken cancellationToken);

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteInLock(
                    LockType.TriggerAccess, 
                    conn => ResumeTriggers(conn, matcher, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't resume trigger group '{matcher}': {e.Message}", e);
            }
        }

        protected abstract Task<IReadOnlyCollection<string>> ResumeTriggers(
            TUnitOfWorkConnection conn, 
            GroupMatcher<TriggerKey> groupMatcher, 
            CancellationToken cancellationToken);

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => GetPausedTriggerGroups(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't determine paused trigger groups: {e.Message}", e);
            }
        }

        protected abstract Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                var triggers = await GetTriggersForJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                foreach (IOperableTrigger trigger in triggers)
                {
                    await ResumeTrigger(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                }
            }, cancellationToken);
        }

        /// <inheritdoc />
        public Task<IReadOnlyCollection<string>> ResumeJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock<IReadOnlyCollection<string>>(
                LockType.TriggerAccess,
                async conn =>
                {
                    IReadOnlyCollection<JobKey> jobKeys = await GetJobKeys(conn, matcher, cancellationToken).ConfigureAwait(false);
                    var groupNames = new ReadOnlyCompatibleHashSet<string>();

                    foreach (JobKey jobKey in jobKeys)
                    {
                        var triggers = await GetTriggersForJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                        foreach (IOperableTrigger trigger in triggers)
                        {
                            await ResumeTrigger(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                        }

                        groupNames.Add(jobKey.Group);
                    }
                    return groupNames;
                }, cancellationToken);
        }

        public Task PauseAll(CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                async conn =>
                {
                    try
                    {
                        await PauseAll(conn, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException($"Couldn't pause all trigger groups: {e.Message}", e);
                    }
                },
                cancellationToken);
        }

        protected abstract Task PauseAll(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public Task ResumeAll(CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess, async conn =>
                {
                    try
                    {
                        var triggerGroupNames = await GetTriggerGroupNames(conn, cancellationToken).ConfigureAwait(false);

                        foreach (string groupName in triggerGroupNames)
                        {
                            await ResumeTriggers(conn, GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken).ConfigureAwait(false);
                        }

                        await ClearAllTriggerGroupsPausedFlag(conn, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("Couldn't resume all trigger groups: " + e.Message, e);
                    }
                }, cancellationToken);
        }

        protected abstract Task ClearAllTriggerGroupsPausedFlag(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public abstract Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public Task ReleaseAcquiredTrigger(
            IOperableTrigger trigger,
            CancellationToken cancellationToken = default)
        {
            return RetryExecuteInNonManagedTXLock(
                LockType.TriggerAccess, async conn =>
                {
                    try
                    {
                        await UpdateTriggerStateFromOtherStates(
                            conn,
                            trigger.Key,
                            InternalTriggerState.Waiting, 
                            new [] { InternalTriggerState.Acquired },
                            cancellationToken).ConfigureAwait(false);

                        await DeleteFiredTrigger(conn, trigger.FireInstanceId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("Couldn't release acquired trigger: " + e.Message, e);
                    }
                },
                cancellationToken);
        }

        /// <inheritdoc />
        public abstract Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<bool> ReplaceTrigger(
            TriggerKey triggerKey, 
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteInLock(
                    LockType.TriggerAccess,
                    async conn =>
                    {
                        IJobDetail job = await GetJobForTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false);

                        if (job == null)
                        {
                            return false;
                        }

                        if (!newTrigger.JobKey.Equals(job.Key))
                        {
                            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                        }

                        bool removedTrigger = await DeleteTriggerAndChildren(conn, triggerKey, cancellationToken).ConfigureAwait(false);

                        await StoreTrigger(conn, newTrigger, job, false, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);

                        return removedTrigger;
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't replace trigger: {e.Message}", e);
            }
        }

        protected abstract Task<IJobDetail> GetJobForTrigger(
            TUnitOfWorkConnection conn, 
            TriggerKey triggerKey,
            CancellationToken cancellationToken);

        protected abstract Task<bool> DeleteTriggerAndChildren(
            TUnitOfWorkConnection conn, 
            TriggerKey triggerKey, 
            CancellationToken cancellationToken);
        
        protected abstract Task<bool> DeleteJobAndChildren(
            TUnitOfWorkConnection conn, 
            JobKey jobKey, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<IOperableTrigger> RetrieveTrigger(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => RetrieveTrigger(conn, triggerKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't retrieve trigger: {e.Message}", e);
            }
        }

        /// <inheritdoc />
        public async Task<bool> CalendarExists(
            string calendarName, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => CalendarExists(conn, calendarName, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't check for existence of calendar: {e.Message}", e);
            }
        }

        protected abstract Task<bool> CalendarExists(
            TUnitOfWorkConnection conn,
            string calendarName,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<bool> CheckExists(
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => CheckExists(conn, jobKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't check for existence of job: {e.Message}", e);
            }
        }

        protected abstract Task<bool> CheckExists(
            TUnitOfWorkConnection conn,
            JobKey jobKey, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task<bool> CheckExists(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await ExecuteWithoutLock(
                    conn => CheckExists(conn, triggerKey, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't check for existence of trigger: {e.Message}", e);
            }
        }

        protected abstract Task<bool> CheckExists(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey, 
            CancellationToken cancellationToken);

        /// <inheritdoc />
        public async Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            try
            {
                await ExecuteInLock(
                    LockType.TriggerAccess,
                    conn => ClearAllSchedulingData(conn, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Error clearing scheduling data: {e.Message}", e);
            }
        }

        protected abstract Task ClearAllSchedulingData(
            TUnitOfWorkConnection conn, 
            CancellationToken cancellationToken);

        public async Task StoreCalendar(
            string name, 
            ICalendar calendar, 
            bool replaceExisting, 
            bool updateTriggers, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                await ExecuteInLock(
                    LockOnInsert || updateTriggers ? LockType.TriggerAccess : LockType.None,
                    conn => StoreCalendar(conn, name, calendar, replaceExisting, updateTriggers, cancellationToken),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (IOException e)
            {
                throw new JobPersistenceException(
                    "Couldn't store calendar because the BLOB couldn't be serialized: " + e.Message, e);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't store calendar: " + e.Message, e);
            }
        }

        protected abstract Task StoreCalendar(
            TUnitOfWorkConnection conn,
            string name,
            ICalendar calendar, 
            bool replaceExisting, 
            bool updateTriggers, 
            CancellationToken cancellationToken);

        protected abstract Task<IOperableTrigger> RetrieveTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey, 
            CancellationToken cancellationToken);

        public Task StoreTrigger(
            IOperableTrigger newTrigger,
            bool replaceExisting,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => StoreTrigger(conn, newTrigger, null, replaceExisting, InternalTriggerState.Waiting, false, false, cancellationToken),
                cancellationToken);
        }

        protected abstract Task StoreTrigger(
            TUnitOfWorkConnection conn,
            IOperableTrigger newTrigger,
            IJobDetail job,
            bool replaceExisting,
            InternalTriggerState state,
            bool forceState,
            bool recovering,
            CancellationToken cancellationToken = default);
        

        public async Task<bool> RemoveTriggers(
            IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken cancellationToken = default)
        {
            return await ExecuteInLock(
                LockType.TriggerAccess,
                async conn =>
                {
                    bool allFound = true;

                    // TODO: make this more efficient with a true bulk operation...
                    foreach (TriggerKey triggerKey in triggerKeys)
                    {
                        allFound = await RemoveTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false) && allFound;
                    }

                    return allFound;
                }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task<bool> RemoveTrigger(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => RemoveTrigger(conn, triggerKey, null, cancellationToken),
                cancellationToken);
        }
        
        protected Task<bool> RemoveTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default) => RemoveTrigger(conn, triggerKey, null, cancellationToken);

        protected abstract Task<bool> RemoveTrigger(
            TUnitOfWorkConnection conn,
            TriggerKey triggerKey,
            IJobDetail job,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public Task<IJobDetail> RetrieveJob(
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock(async conn =>
            {
                try
                {
                    return await RetrieveJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                }
                catch (TypeLoadException e)
                {
                    throw new JobPersistenceException("Couldn't retrieve job because a required type was not found: " + e.Message, e);
                }
                catch (IOException e)
                {
                    throw new JobPersistenceException("Couldn't retrieve job because the BLOB couldn't be deserialized: " + e.Message, e);
                }
                catch (Exception e)
                {
                    throw new JobPersistenceException("Couldn't retrieve job: " + e.Message, e);
                }
            }, cancellationToken);
        }

        protected abstract Task<IJobDetail> RetrieveJob(
            TUnitOfWorkConnection conn,
            JobKey jobKey,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<bool> RemoveJobs(
            IReadOnlyCollection<JobKey> jobKeys,
            CancellationToken cancellationToken = default)
        {
            return await ExecuteInLock(
                LockType.TriggerAccess,
                async conn =>
                {
                    bool allFound = true;

                    // TODO: make this more efficient with a true bulk operation...
                    foreach (JobKey jobKey in jobKeys)
                    {
                        allFound = await RemoveJob(conn, jobKey, cancellationToken).ConfigureAwait(false) && allFound;
                    }

                    return allFound;
                }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task<bool> RemoveJob(
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess, 
                conn => RemoveJob(conn, jobKey, cancellationToken),
                cancellationToken);
        }

        private async Task<bool> RemoveJob(
            TUnitOfWorkConnection conn,
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var jobTriggers = await GetTriggerNamesForJob(conn, jobKey, cancellationToken).ConfigureAwait(false);

                foreach (TriggerKey jobTrigger in jobTriggers)
                {
                    await DeleteTriggerAndChildren(conn, jobTrigger, cancellationToken).ConfigureAwait(false);
                }

                return await DeleteJobAndChildren(conn, jobKey, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't remove job: {e.Message}", e);
            }
        }

        protected abstract Task<IReadOnlyCollection<TriggerKey>> GetTriggerNamesForJob(TUnitOfWorkConnection conn,
            JobKey jobKey,
            CancellationToken cancellationToken);

        /// <summary>
        /// Determines if a Trigger for the given job should be blocked.
        /// State can only transition to StatePausedBlocked/StateBlocked from
        /// StatePaused/StateWaiting respectively.
        /// </summary>
        /// <returns>StatePausedBlocked, StateBlocked, or the currentState. </returns>
        protected virtual async Task<InternalTriggerState> CheckBlockedState(
            TUnitOfWorkConnection conn,
            JobKey jobKey,
            InternalTriggerState currentState,
            CancellationToken cancellationToken = default)
        {
            // State can only transition to BLOCKED from PAUSED or WAITING.
            if (currentState != InternalTriggerState.Waiting && currentState != InternalTriggerState.Paused)
            {
                return currentState;
            }

            try
            {
                var lst = await SelectFiredTriggerRecordsByJob(conn, jobKey, cancellationToken).ConfigureAwait(false);

                if (lst.Count > 0)
                {
                    FiredTriggerRecord rec = lst.First();
                    if (rec.JobDisallowsConcurrentExecution) // TODO: worry about failed/recovering/volatile job  states?
                    {
                        return InternalTriggerState.Paused == currentState
                            ? InternalTriggerState.PausedAndBlocked
                            : InternalTriggerState.Blocked;
                    }
                }

                return currentState;
            }
            catch (Exception e)
            {
                var message = $"Couldn't determine if trigger should be in a blocked state '{jobKey}': {e.Message}";
                throw new JobPersistenceException(message, e);
            }
        }

        protected abstract Task<IReadOnlyCollection<FiredTriggerRecord>> SelectFiredTriggerRecordsByJob(
            TUnitOfWorkConnection conn,
            JobKey jobKey,
            CancellationToken cancellationToken);

        /// <summary>
        /// Execute the given callback in a transaction. Depending on the JobStore,
        /// the surrounding transaction may be assumed to be already present (managed).
        /// </summary>
        /// <remarks>This method just forwards to ExecuteInLock() with a null lockName.</remarks>
        protected Task<T> ExecuteWithoutLock<T>(
            Func<TUnitOfWorkConnection, Task<T>> txCallback,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.None, txCallback, cancellationToken);
        }

        /// <summary>
        /// Execute the given callback in a transaction. Depending on the JobStore,
        /// the surrounding transaction may be assumed to be already present (managed).
        /// </summary>
        protected async Task ExecuteInLock(
            LockType lockName,
            Func<TUnitOfWorkConnection, Task> txCallback,
            CancellationToken cancellationToken = default)
        {
            await ExecuteInLock<object>(lockName, async conn =>
            {
                await txCallback(conn).ConfigureAwait(false);
                return null;
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute the given callback having acquired the given lock.
        /// Depending on the JobStore, the surrounding transaction may be
        /// assumed to be already present (managed).
        /// </summary>
        protected abstract Task<T> ExecuteInLock<T>(
            LockType lockName, 
            Func<TUnitOfWorkConnection, Task<T>> txCallback, 
            CancellationToken cancellationToken);
    }
}