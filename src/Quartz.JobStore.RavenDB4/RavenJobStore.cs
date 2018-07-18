using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Quartz.Core;
using Quartz.Impl.Matchers;
using Quartz.Logging;
using Quartz.Spi;
using Quartz.Simpl;

using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;

namespace Quartz.Impl.RavenDB
{
    /// <summary> 
    /// An implementation of <see cref="IJobStore" /> to use ravenDB as a persistent Job Store.
    /// Mostly based on RAMJobStore logic with changes to support persistent storage.
    /// Provides an <see cref="IJob" />
    /// and <see cref="ITrigger" /> storage mechanism for the
    /// <see cref="QuartzScheduler" />'s use.
    /// </summary>
    /// <remarks>
    /// Storage of <see cref="IJob" /> s and <see cref="ITrigger" /> s should be keyed
    /// on the combination of their name and group for uniqueness.
    /// </remarks>
    /// <seealso cref="QuartzScheduler" />
    /// <seealso cref="IJobStore" />
    /// <seealso cref="ITrigger" />
    /// <seealso cref="IJob" />
    /// <seealso cref="IJobDetail" />
    /// <seealso cref="JobDataMap" />
    /// <seealso cref="ICalendar" />
    /// <author>Iftah Ben Zaken</author>
    /// <author>Marko Lahma (RavenDB 4 port)</author>
    public class RavenJobStore : IJobStore
    {
        private const string LockTriggerAccess = "TRIGGER_ACCESS";
        private const string LockStateAccess = "STATE_ACCESS";
        private IRavenLockHandler lockHandler;

        private TimeSpan misfireThreshold = TimeSpan.FromMinutes(1);
        private TimeSpan? misfirehandlerFrequence;
        
        private ISchedulerSignaler schedulerSignaler;
        private static long ftrCtr = SystemTime.UtcNow().Ticks;

        private DocumentStore documentStore;
        private MisfireHandler misfireHandler;

#pragma warning disable 414
        private bool schedulerRunning;
        private bool shutdown;
#pragma warning restore 414

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;

        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public static string Url { get; set; }
        public string Database { get; set; }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            get { return misfireThreshold; }
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
        public virtual TimeSpan MisfireHandlerFrequency
        {
            get { return misfirehandlerFrequence.GetValueOrDefault(MisfireThreshold); }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireHandlerFrequency must be larger than 0");
                }

                misfirehandlerFrequence = value;
            }
        }

        /// <summary>
        /// Get whether the threads spawned by this JobStore should be
        /// marked as daemon.  Possible threads include the <see cref="MisfireHandler" />
        /// and the <see cref="ClusterManager"/>.
        /// </summary>
        /// <returns></returns>
        public bool MakeThreadsDaemons { get; set; }

        private ILog Log { get; }


        private ClusterManager clusterManager;

        public bool Clustered
        {
            get => false;
            // ReSharper disable once UnusedMember.Global
            set => throw new NotImplementedException("Clustering support hasn't been implemented yet");
        }

        public int RetryableActionErrorLogThreshold
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public TimeSpan ClusterCheckinInterval
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public TimeSpan DbRetryInterval
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public DateTimeOffset LastCheckin
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public RavenJobStore()
        {
            Log = LogProvider.GetLogger(GetType());
        }

        /// <inheritdoc />
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s, CancellationToken cancellationToken = default)
        {
            schedulerSignaler = s;

            if (string.IsNullOrWhiteSpace(Url))
            {
                throw new ConfigurationErrorsException("url is not defined");
            }

            if (string.IsNullOrWhiteSpace(Database))
            {
                throw new ConfigurationErrorsException("database is not defined");
            }

            documentStore = new DocumentStore {Urls = new[] {Url}, Database = Database};
            documentStore.OnBeforeQuery += (sender, beforeQueryExecutedArgs) => { beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults(); };
            documentStore.Initialize();
            

            if ("a".StartsWith("b"))
            {
                Log.Info("Using db table-based data access locking (synchronization).");
                lockHandler = new RavenLockHandler(InstanceName);
            }
            else
            {
                Log.Info("Using thread monitor-based data access locking (synchronization).");
                lockHandler = new SimpleSemaphoreRavenLockHandler();
            }

            await new TriggerIndex().ExecuteAsync(documentStore, token: cancellationToken, database: Database);
            await new JobIndex().ExecuteAsync(documentStore, token: cancellationToken, database: Database);

            // If scheduler doesn't exist create new empty scheduler and store it
            var scheduler = new Scheduler
            {
                InstanceName = InstanceName,
                State = "Started"
            };

            using (var session = documentStore.OpenAsyncSession())
            {
                await session.StoreAsync(scheduler, InstanceName, cancellationToken);
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            if (Clustered)
            {
                clusterManager = new ClusterManager(this);
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

            misfireHandler = new MisfireHandler(this);
            misfireHandler.Initialize();
            schedulerRunning = true;

            await SetSchedulerState("Started", cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            return SetSchedulerState("Paused", cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            return SetSchedulerState("Resumed", cancellationToken);
        }

        /// <inheritdoc />
        public async Task Shutdown(CancellationToken cancellationToken = default)
        {
            shutdown = true;

            if (misfireHandler != null)
            {
                await misfireHandler.Shutdown().ConfigureAwait(false);
            }

            if (clusterManager != null)
            {
                await clusterManager.Shutdown().ConfigureAwait(false);
            }

            await SetSchedulerState("Shutdown", cancellationToken);
            
            documentStore.Dispose();
            documentStore = null;
        }

        private async Task SetSchedulerState(string state, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.State = state;
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as
        /// appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        private Task RecoverJobs(CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockTriggerAccess,
                session => RecoverJobs(session, cancellationToken),
                cancellationToken);
        }

        private async Task RecoverJobs(RavenConnection conn, CancellationToken cancellationToken = default)
        {
            try
            {
                Log.Info("Trying to recover persisted scheduler data for" + InstanceName);

                // update inconsistent states

                var queryResult = await conn.Query<Trigger, TriggerIndex>()
                    .Where(x => x.Scheduler == InstanceName && (x.State == InternalTriggerState.Acquired || x.State == InternalTriggerState.Blocked))
                    .ToListAsync(cancellationToken);

                foreach (var trigger in queryResult)
                {
                    var triggerToUpdate = await conn.LoadAsync<Trigger>(trigger.Key, cancellationToken);
                    triggerToUpdate.State = InternalTriggerState.Waiting;
                }

                Log.Info("Freed triggers from 'acquired' / 'blocked' state.");

                // recover jobs marked for recovery that were not fully executed
                List<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                var queryResultJobs = await conn.Query<Job, JobIndex>()
                    .Where(x => x.Scheduler == InstanceName && x.RequestsRecovery)
                    .ToListAsync(cancellationToken);

                foreach (var job in queryResultJobs)
                {
                    var triggers = await GetTriggersForJob(new JobKey(job.Name, job.Group), cancellationToken);
                    recoveringJobTriggers.AddRange(triggers);
                }

                Log.Info("Recovering " + recoveringJobTriggers.Count +
                         " jobs that were in-progress at the time of the last shut-down.");

                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (await CheckExists(trigger.JobKey, cancellationToken))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(trigger, true, cancellationToken);
                    }
                }

                Log.Info("Recovery complete.");

                // remove lingering 'complete' triggers...
                Log.Info("Removing 'complete' triggers...");

                using (var session = documentStore.OpenAsyncSession())
                {
                    var triggersInStateComplete = await session.Query<Trigger, TriggerIndex>()
                        .Where(x => x.Scheduler == InstanceName && x.State == InternalTriggerState.Complete)
                        .Select(x => new {x.Name, x.Group})
                        .ToListAsync(cancellationToken);

                    foreach (var trigger in triggersInStateComplete)
                    {
                        await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group), cancellationToken);
                    }
                }

                var scheduler = await conn.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.State = "Started";
            }
            catch (JobPersistenceException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
            }
        }

        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        private static string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public async Task StoreJobAndTrigger(
            IJobDetail newJob,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            await StoreJob(newJob, true, cancellationToken);
            await StoreTrigger(newTrigger, true, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                return scheduler.PausedJobGroups.Contains(groupName);
            }
        }

        /// <inheritdoc />
        public async Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            return (await GetPausedTriggerGroups(cancellationToken)).Contains(groupName);
        }

        /// <inheritdoc />
        public async Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            if (await CheckExists(newJob.Key, cancellationToken))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
            }

            var job = new Job(newJob, InstanceName);

            using (var session = documentStore.OpenAsyncSession())
            {
                // Store() overwrites if job id already exists
                await session.StoreAsync(job, job.Key, cancellationToken);
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
            bool replace,
            CancellationToken cancellationToken = default)
        {
            using (var bulkInsert = documentStore.BulkInsert())
            {
                foreach (var pair in triggersAndJobs)
                {
                    // First store the current job
                    bulkInsert.Store(new Job(pair.Key, InstanceName), pair.Key.Key.DocumentId());

                    // Storing all triggers for the current job
                    foreach (var trig in pair.Value)
                    {
                        if (!(trig is IOperableTrigger operableTrigger))
                        {
                            continue;
                        }

                        var trigger = new Trigger(operableTrigger, InstanceName);

                        if ((await GetPausedTriggerGroups(cancellationToken)).Contains(operableTrigger.Key.Group)
                            || (await GetPausedJobGroups(cancellationToken)).Contains(operableTrigger.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Paused;
                            if ((await GetBlockedJobs(cancellationToken)).Contains(operableTrigger.JobKey.DocumentId()))
                            {
                                trigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        else if ((await GetBlockedJobs(cancellationToken)).Contains(operableTrigger.JobKey.DocumentId()))
                        {
                            trigger.State = InternalTriggerState.Blocked;
                        }

                        bulkInsert.Store(trigger, trigger.Key);
                    }
                }
            } // bulkInsert is disposed - same effect as await session.SaveChangesAsync()

        }

        /// <inheritdoc />
        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                if (!await CheckExists(jobKey, cancellationToken))
                {
                    return false;
                }

                session.Delete(jobKey.DocumentId());
                await session.SaveChangesAsync(cancellationToken);
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            // Returns false in case at least one job removal fails
            var result = true;
            foreach (var key in jobKeys)
            {
                result &= await RemoveJob(key, cancellationToken);
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var job = await session.LoadAsync<Job>(jobKey.DocumentId(), cancellationToken);

                return job?.Deserialize();
            }
        }

        /// <inheritdoc />
        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            if (await CheckExists(newTrigger.Key, cancellationToken))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
            }

            if (!await CheckExists(newTrigger.JobKey, cancellationToken))
            {
                throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
            }

            var trigger = new Trigger(newTrigger, InstanceName);

            // make sure trigger group is not paused and that job is not blocked
            if ((await GetPausedTriggerGroups(cancellationToken)).Contains(newTrigger.Key.Group)
                || (await GetPausedJobGroups(cancellationToken)).Contains(newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Paused;
                if ((await GetBlockedJobs(cancellationToken)).Contains(newTrigger.JobKey.DocumentId()))
                {
                    trigger.State = InternalTriggerState.PausedAndBlocked;
                }
            }
            else if ((await GetBlockedJobs(cancellationToken)).Contains(newTrigger.JobKey.DocumentId()))
            {
                trigger.State = InternalTriggerState.Blocked;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                // Overwrite if exists
                await session.StoreAsync(trigger, trigger.Key, cancellationToken);
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.DocumentId(), cancellationToken);

                if (trigger == null)
                {
                    return false;
                }

                var job = await RetrieveJob(new JobKey(trigger.JobName, trigger.Group), cancellationToken);

                // Delete trigger
                session.Delete(trigger);
                await session.SaveChangesAsync(cancellationToken);

                // Remove the trigger's job if it is not associated with any other triggers
                var trigList = await GetTriggersForJob(job.Key, cancellationToken);
                if ((trigList == null || trigList.Count == 0) && !job.Durable)
                {
                    if (await RemoveJob(job.Key, cancellationToken))
                    {
                        await schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken);
                    }
                }
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<bool> RemoveTriggers(
            IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken cancellationToken = default)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            foreach (var key in triggerKeys)
            {
                result &= await RemoveTrigger(key, cancellationToken);
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<bool> ReplaceTrigger(
            TriggerKey triggerKey,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken))
            {
                return false;
            }

            var wasRemoved = await RemoveTrigger(triggerKey, cancellationToken);
            if (wasRemoved)
            {
                await StoreTrigger(newTrigger, true, cancellationToken);
            }

            return wasRemoved;
        }

        /// <inheritdoc />
        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken))
            {
                return null;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.DocumentId(), cancellationToken);
                return trigger?.Deserialize();
            }
        }

        /// <inheritdoc />
        public async Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            bool answer;
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                if (scheduler == null)
                {
                    return false;
                }

                try
                {
                    answer = scheduler.Calendars.ContainsKey(calName);
                }
                catch (ArgumentNullException argumentNullException)
                {
                    Log.ErrorException("Calendars collection is null.", argumentNullException);
                    answer = false;
                }
            }

            return answer;
        }

        /// <inheritdoc />
        public async Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Advanced.ExistsAsync(jobKey.DocumentId());
            }
        }

        /// <inheritdoc />
        public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Advanced.ExistsAsync(triggerKey.DocumentId());
            }
        }

        /// <inheritdoc />
        public async Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.Calendars.Clear();
                scheduler.PausedJobGroups.Clear();

                await session.SaveChangesAsync(cancellationToken);
            }

            var op = await documentStore.Operations.SendAsync(
                new DeleteByQueryOperation<Trigger, TriggerIndex>(x => x.Key != null),
                token: cancellationToken);

            op.WaitForCompletion();

            op = await documentStore.Operations.SendAsync(
                new DeleteByQueryOperation<Job, JobIndex>(x => x.Key != null),
                token: cancellationToken);

            op.WaitForCompletion();
        }

        /// <inheritdoc />
        public async Task StoreCalendar(
            string name,
            ICalendar calendar,
            bool replaceExisting,
            bool updateTriggers,
            CancellationToken cancellationToken = default)
        {
            var calendarCopy = calendar.Clone();

            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                if (scheduler.Calendars.ContainsKey(name) && !replaceExisting)
                {
                    throw new ObjectAlreadyExistsException($"Calendar with name '{name}' already exists.");
                }

                // add or replace calendar
                scheduler.Calendars[name] = calendarCopy;

                if (!updateTriggers)
                {
                    return;
                }

                var triggersKeysToUpdate = await session
                    .Query<Trigger, TriggerIndex>()
                    .Where(t => t.CalendarName == name)
                    .Select(x => x.Key)
                    .ToListAsync(cancellationToken);

                if (triggersKeysToUpdate.Count == 0)
                {
                    await session.SaveChangesAsync(cancellationToken);
                    return;
                }

                foreach (var triggerKey in triggersKeysToUpdate)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(triggerKey, cancellationToken);
                    var trigger = triggerToUpdate.Deserialize();
                    trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);
                    triggerToUpdate.UpdateFireTimes(trigger);

                }

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            if (await RetrieveCalendar(calName, cancellationToken) == null)
            {
                return false;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.Calendars.Remove(calName);
                await session.SaveChangesAsync(cancellationToken);
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            var callCollection = await RetrieveCalendarCollection(cancellationToken);
            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        private async Task<IReadOnlyDictionary<string, ICalendar>> RetrieveCalendarCollection(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                if (scheduler == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }

                if (scheduler.Calendars == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                }

                return scheduler.Calendars;
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Query<Job, JobIndex>().CountAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Query<Trigger, TriggerIndex>().CountAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken)).Count;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<JobKey>();

            using (var session = documentStore.OpenAsyncSession())
            {
                var allJobs = await session.Query<Job, JobIndex>()
                    .Select(x => new {x.Name, x.Group})
                    .ToListAsync(cancellationToken);

                foreach (var job in allJobs)
                {
                    if (op.Evaluate(job.Group, compareToValue))
                    {
                        result.Add(new JobKey(job.Name, job.Group));
                    }
                }
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<TriggerKey>();

            using (var session = documentStore.OpenAsyncSession())
            {
                var allTriggers = await session.Query<Trigger, TriggerIndex>()
                    .Select(x => new {x.Name, x.Group})
                    .ToListAsync(cancellationToken);

                foreach (var trigger in allTriggers)
                {
                    if (op.Evaluate(trigger.Group, compareToValue))
                    {
                        result.Add(new TriggerKey(trigger.Name, trigger.Group));
                    }
                }
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Query<Job, JobIndex>()
                    .Select(x => x.Group)
                    .Distinct()
                    .ToListAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var result = await session.Query<Trigger, TriggerIndex>()
                    .Select(x => x.Group)
                    .Distinct()
                    .ToListAsync(cancellationToken);

                return result;
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken)).Keys.ToList();
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                try
                {
                    var triggers = await session
                        .Query<Trigger, TriggerIndex>()
                        .Where(x => x.JobName == jobKey.Name && x.Group == jobKey.Group)
                        .ToListAsync(cancellationToken);

                    var result = triggers
                        .Select(x => x.Deserialize())
                        .ToList();

                    return result;
                }
                catch (NullReferenceException)
                {
                    return new List<IOperableTrigger>();
                }
            }
        }

        /// <inheritdoc />
        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            Trigger trigger;
            using (var session = documentStore.OpenAsyncSession())
            {
                trigger = await session.LoadAsync<Trigger>(triggerKey.DocumentId(), cancellationToken);
            }

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case InternalTriggerState.Complete:
                    return TriggerState.Complete;
                case InternalTriggerState.Paused:
                    return TriggerState.Paused;
                case InternalTriggerState.PausedAndBlocked:
                    return TriggerState.Paused;
                case InternalTriggerState.Blocked:
                    return TriggerState.Blocked;
                case InternalTriggerState.Error:
                    return TriggerState.Error;
                default:
                    return TriggerState.Normal;
            }
        }

        /// <inheritdoc />
        public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken))
            {
                return;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var trig = await session.LoadAsync<Trigger>(triggerKey.DocumentId(), cancellationToken);

                // if the trigger doesn't exist or is "complete" pausing it does not make sense...
                if (trig == null)
                {
                    return;
                }

                if (trig.State == InternalTriggerState.Complete)
                {
                    return;
                }

                trig.State = trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused;
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> PauseTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher, cancellationToken);
            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey, cancellationToken);
                pausedGroups.Add(triggerKey.Group);
            }

            return new HashSet<string>(pausedGroups);
        }

        /// <inheritdoc />
        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                await PauseTrigger(trigger.Key, cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> PauseJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = await GetJobKeys(matcher, cancellationToken);
            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey, cancellationToken);
                pausedGroups.Add(jobKey.Group);

                using (var session = documentStore.OpenAsyncSession())
                {
                    var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                    scheduler.PausedJobGroups.Add(matcher.CompareToValue);
                    await session.SaveChangesAsync(cancellationToken);
                }
            }

            return pausedGroups;
        }

        /// <inheritdoc />
        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken))
            {
                return;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.DocumentId(), cancellationToken);

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                trigger.State = (await GetBlockedJobs(cancellationToken)).Contains(trigger.JobKey) ? InternalTriggerState.Blocked : InternalTriggerState.Waiting;

                await ApplyMisfire(trigger);

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> ResumeTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetTriggerKeys(matcher, cancellationToken);

            foreach (TriggerKey triggerKey in keys)
            {
                await ResumeTrigger(triggerKey, cancellationToken);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var groups = await session.Query<Trigger, TriggerIndex>()
                    .Where(x => x.State == InternalTriggerState.Paused || x.State == InternalTriggerState.PausedAndBlocked)
                    .Select(x => x.Group)
                    .Distinct()
                    .ToListAsync(cancellationToken);

                return groups;
            }
        }

        /// <summary>
        /// Gets the paused job groups.
        /// </summary>
        private async Task<HashSet<string>> GetPausedJobGroups(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                return scheduler.PausedJobGroups;
            }
        }

        /// <summary>
        /// Gets the blocked jobs set.
        /// </summary>
        private async Task<HashSet<string>> GetBlockedJobs(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                return scheduler.BlockedJobs;
            }
        }

        /// <inheritdoc />
        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                await ResumeTrigger(trigger.Key, cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> ResumeJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetJobKeys(matcher, cancellationToken);

            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                foreach (var pausedJobGroup in scheduler.PausedJobGroups)
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                foreach (var resumedGroup in resumedGroups)
                {
                    scheduler.PausedJobGroups.Remove(resumedGroup);
                }

                await session.SaveChangesAsync(cancellationToken);
            }

            foreach (JobKey key in keys)
            {
                var triggers = await GetTriggersForJob(key, cancellationToken);
                foreach (IOperableTrigger trigger in triggers)
                {
                    await ResumeTrigger(trigger.Key, cancellationToken);
                }
            }

            return resumedGroups;
        }

        /// <inheritdoc />
        public async Task PauseAll(CancellationToken cancellationToken = default)
        {
            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
            {
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task ResumeAll(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.PausedJobGroups.Clear();

                var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

                foreach (var groupName in triggerGroupNames)
                {
                    await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
                }
            }
        }

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="trigger">The trigger wrapper.</param>
        /// <returns></returns>
        protected virtual async Task<bool> ApplyMisfire(Trigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? nextFireTimeUtc = trigger.NextFireTimeUtc;
            if (!nextFireTimeUtc.HasValue || nextFireTimeUtc.Value > misfireTime
                                          || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = await RetrieveCalendar(trigger.CalendarName);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            await schedulerSignaler.NotifyTriggerListenersMisfired(trig);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await schedulerSignaler.NotifySchedulerListenersFinalized(trig);
                trigger.State = InternalTriggerState.Complete;

            }
            else if (nextFireTimeUtc.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        /// <inheritdoc />
        public virtual async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = default)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            using (var session = documentStore.OpenAsyncSession())
            {
                var cutOff = noLaterThan + timeWindow;
                var triggersQuery = await session.Query<Trigger, TriggerIndex>()
                    .Where(t => t.State == InternalTriggerState.Waiting && t.NextFireTimeUtc <= cutOff)
                    .OrderBy(t => t.NextFireTimeTicks)
                    .ThenByDescending(t => t.Priority)
                    .ToListAsync(cancellationToken);

                var triggers = new SortedSet<Trigger>(triggersQuery, new TriggerComparator());

                while (true)
                {
                    // return empty list if store has no such triggers.
                    if (!triggers.Any())
                    {
                        return result;
                    }

                    var candidateTrigger = triggers.First();
                    if (candidateTrigger == null)
                    {
                        break;
                    }

                    if (!triggers.Remove(candidateTrigger))
                    {
                        break;
                    }

                    if (candidateTrigger.NextFireTimeUtc == null)
                    {
                        continue;
                    }

                    if (await ApplyMisfire(candidateTrigger))
                    {
                        if (candidateTrigger.NextFireTimeUtc != null)
                        {
                            triggers.Add(candidateTrigger);
                        }

                        continue;
                    }

                    if (candidateTrigger.NextFireTimeUtc > noLaterThan + timeWindow)
                    {
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = new JobKey(candidateTrigger.JobName, candidateTrigger.Group);
                    Job job = await session.LoadAsync<Job>(candidateTrigger.JobKey, cancellationToken);

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue; // go to next trigger in store.
                        }

                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }

                    candidateTrigger.State = InternalTriggerState.Acquired;
                    candidateTrigger.FireInstanceId = GetFiredTriggerRecordId();

                    result.Add(candidateTrigger.Deserialize());

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = candidateTrigger.NextFireTimeUtc;
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }

                await session.SaveChangesAsync(cancellationToken);
            }

            return result;
        }

        /// <inheritdoc />
        public async Task ReleaseAcquiredTrigger(IOperableTrigger trig, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(trig.Key.DocumentId(), cancellationToken);
                if (trigger == null || trigger.State != InternalTriggerState.Acquired)
                {
                    return;
                }

                trigger.State = InternalTriggerState.Waiting;
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = default)
        {
            var results = new List<TriggerFiredResult>();
            using (var session = documentStore.OpenAsyncSession())
            {
                try
                {
                    foreach (IOperableTrigger tr in triggers)
                    {
                        // was the trigger deleted since being acquired?
                        var trigger = await session.LoadAsync<Trigger>(tr.Key.DocumentId(), cancellationToken);

                        // was the trigger completed, paused, blocked, etc. since being acquired?
                        if (trigger?.State != InternalTriggerState.Acquired)
                        {
                            continue;
                        }

                        ICalendar cal = null;
                        if (trigger.CalendarName != null)
                        {
                            cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);
                            if (cal == null)
                            {
                                continue;
                            }
                        }

                        DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;

                        var trig = trigger.Deserialize();
                        trig.Triggered(cal);

                        TriggerFiredBundle bundle = new TriggerFiredBundle(
                            await RetrieveJob(trig.JobKey, cancellationToken),
                            trig,
                            cal,
                            false, SystemTime.UtcNow(),
                            trig.GetPreviousFireTimeUtc(), prevFireTime,
                            trig.GetNextFireTimeUtc());

                        IJobDetail job = bundle.JobDetail;

                        trigger.UpdateFireTimes(trig);
                        trigger.State = InternalTriggerState.Waiting;

                        if (job.ConcurrentExecutionDisallowed)
                        {
                            var dbTriggers = await session.Query<Trigger, TriggerIndex>()
                                .Where(x => x.Group == job.Key.Group && x.JobName == job.Key.Name)
                                .ToListAsync(cancellationToken);

                            foreach (var t in dbTriggers)
                            {
                                if (t.State == InternalTriggerState.Waiting)
                                {
                                    t.State = InternalTriggerState.Blocked;
                                }

                                if (t.State == InternalTriggerState.Paused)
                                {
                                    t.State = InternalTriggerState.PausedAndBlocked;
                                }
                            }

                            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                            scheduler.BlockedJobs.Add(job.Key.DocumentId());
                        }

                        results.Add(new TriggerFiredResult(bundle));
                    }
                }
                finally
                {
                    await session.SaveChangesAsync(cancellationToken);
                }
            }

            return results;

        }

        /// <inheritdoc />
        public async Task TriggeredJobComplete(
            IOperableTrigger trig,
            IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(trig.Key.DocumentId(), cancellationToken);
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                // It's possible that the job or trigger is null if it was deleted during execution
                var job = await session.LoadAsync<Job>(trig.JobKey.DocumentId(), cancellationToken);

                if (job != null)
                {
                    if (jobDetail.PersistJobDataAfterExecution)
                    {
                        job.JobDataMap = jobDetail.JobDataMap;

                    }

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        scheduler.BlockedJobs.Remove(job.Key);

                        var triggers = await session.Query<Trigger, TriggerIndex>()
                            .Where(x => x.Group == job.Group && x.JobName == job.Name)
                            .ToListAsync(cancellationToken);

                        foreach (Trigger t in triggers)
                        {
                            var triggerToUpdate = await session.LoadAsync<Trigger>(t.Key, cancellationToken);
                            if (t.State == InternalTriggerState.Blocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Waiting;
                            }

                            if (t.State == InternalTriggerState.PausedAndBlocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Paused;
                            }
                        }

                        schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                    }
                }
                else
                {
                    // even if it was deleted, there may be cleanup to do
                    scheduler.BlockedJobs.Remove(jobDetail.Key.DocumentId());
                }

                // check for trigger deleted during execution...
                if (trigger != null)
                {
                    if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                    {
                        // Deleting triggers
                        DateTimeOffset? d = trig.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = trigger.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                await RemoveTrigger(trig.Key, cancellationToken);
                            }
                            else
                            {
                                //Deleting cancelled - trigger still active
                            }
                        }
                        else
                        {
                            await RemoveTrigger(trig.Key, cancellationToken);
                            schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        trigger.State = InternalTriggerState.Complete;
                        schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        trigger.State = InternalTriggerState.Error;
                        schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Error, cancellationToken);
                        schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Complete, cancellationToken);
                        schedulerSignaler.SignalSchedulingChange(null, cancellationToken);
                    }
                }

                await session.SaveChangesAsync(cancellationToken);
            }
        }


        /// <summary>
        /// Sets the State of all triggers of job to specified State.
        /// </summary>
        protected virtual async Task SetAllTriggersOfJobToState(
            JobKey jobKey,
            InternalTriggerState state,
            CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var triggers = await session.Query<Trigger, TriggerIndex>()
                    .Where(x => x.Group == jobKey.Group && x.JobName == jobKey.Name)
                    .Select(x => new {x.Key})
                    .ToListAsync(cancellationToken);

                foreach (var trig in triggers)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(trig.Key, cancellationToken);
                    triggerToUpdate.State = state;
                }

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        private Task ExecuteInLock(
            string lockName,
            Func<RavenConnection, Task> txCallback,
            CancellationToken cancellationToken)
        {
            return ExecuteInLock<object>(lockName, async (conn) =>
            {
                await txCallback(conn);
                return null;
            }, cancellationToken);
        }

        private async Task<T> ExecuteInLock<T>(
            string lockName, 
            Func<RavenConnection, Task<T>> txCallback, 
            CancellationToken cancellationToken)
        {
            bool transOwner = false;
            Guid requestorId = Guid.NewGuid();
            RavenConnection conn = null;
            try
            {
                conn = new RavenConnection(documentStore.OpenAsyncSession());
                if (lockName != null)
                {
                    transOwner = await lockHandler.ObtainLock(requestorId, conn, lockName, cancellationToken).ConfigureAwait(false);
                }

                T result = await txCallback(conn).ConfigureAwait(false);
                await conn.Commit(cancellationToken);
                
                DateTimeOffset? sigTime = conn.SignalSchedulingChangeOnTxCompletion;
                if (sigTime != null)
                {
                    SignalSchedulingChangeImmediately(sigTime);
                }

                return result;
            }
            catch (JobPersistenceException)
            {
                conn?.Rollback();
                throw;
            }
            catch (Exception e)
            {
                conn?.Rollback();
                throw new JobPersistenceException("Unexpected runtime exception: " + e.Message, e);
            }
            finally
            {
                try
                {
                    await lockHandler.ReleaseLock(requestorId, lockName, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    conn?.Rollback();
                }
            }
        }

        public Task<bool> DoCheckin(Guid requestorId)
        {
            throw new NotImplementedException();
        }

        public Task<(bool HasMoreMisfiredTriggers, int ProcessedMisfiredTriggerCount, DateTimeOffset EarliestNewTime)>
            DoRecoverMisfires(Guid requestorId, CancellationToken none)
        {
            throw new NotImplementedException();
        }

        internal void SignalSchedulingChangeImmediately(DateTimeOffset? schedulingSignalDateTime)
        {
            schedulerSignaler.SignalSchedulingChange(schedulingSignalDateTime);
        }

    }
}