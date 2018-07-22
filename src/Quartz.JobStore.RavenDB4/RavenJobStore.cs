using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Quartz.Core;
using Quartz.Impl.Matchers;
using Quartz.Logging;
using Quartz.Spi;
using Quartz.Simpl;
using Quartz.Util;

using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;

namespace Quartz.Impl.RavenDB
{
    /// <summary> 
    /// An implementation of <see cref="IJobStore" /> to use RavenDB as a persistent job store.
    /// Provides an <see cref="IJob" /> and <see cref="ITrigger" /> storage mechanism for the
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
    /// <author>Marko Lahma</author>
    public class RavenJobStore : PersistentJobStore, IJobStore, IClusterManagementOperations, IMisfireHandlerOperations
    {
        private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";

        private IRavenLockHandler lockHandler;

        private DocumentStore documentStore;

        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;

        public static string Url { get; set; }
        public string Database { get; set; }


        public DateTimeOffset LastCheckin
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        protected override void ValidateInstanceName(string value)
        {
            if (string.IsNullOrEmpty(value) || value.IndexOf('/') > -1)
            {
                throw new ArgumentException("scheduler name must be set and it cannot contain '/' character");
            }
        }

        public override async Task Initialize(
            ITypeLoadHelper typeLoadHelper, 
            ISchedulerSignaler schedulerSignaler, 
            CancellationToken cancellationToken = default)
        {
            await base.Initialize(typeLoadHelper, schedulerSignaler, cancellationToken).ConfigureAwait(false);

            if (string.IsNullOrWhiteSpace(Url))
            {
                throw new ConfigurationErrorsException("url is not defined");
            }

            if (string.IsNullOrWhiteSpace(Database))
            {
                throw new ConfigurationErrorsException("database is not defined");
            }

            documentStore = new DocumentStore
            {
                Urls = new[]
                {
                    Url
                },
                Database = Database
            };
            documentStore.OnBeforeQuery += (sender, beforeQueryExecutedArgs) => { beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults(); };
            documentStore.Initialize();

            if ("a".StartsWith("b"))
            {
                Log.Info("Using db based data access locking (synchronization).");
                lockHandler = new RavenLockHandler(InstanceName);
            }
            else
            {
                Log.Info("Using thread monitor-based data access locking (synchronization).");
                lockHandler = new SimpleSemaphoreRavenLockHandler();
            }

            await new FiredTriggerIndex().ExecuteAsync(documentStore, token: cancellationToken, database: Database).ConfigureAwait(false);
            await new JobIndex().ExecuteAsync(documentStore, token: cancellationToken, database: Database).ConfigureAwait(false);
            await new TriggerIndex().ExecuteAsync(documentStore, token: cancellationToken, database: Database).ConfigureAwait(false);

            // If scheduler doesn't exist create new empty scheduler and store it
            var scheduler = new Scheduler
            {
                InstanceName = InstanceName,
                State = SchedulerState.Initialized
            };

            using (var session = documentStore.OpenAsyncSession())
            {
                await session.StoreAsync(scheduler, InstanceName, cancellationToken).ConfigureAwait(false);
                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public override async Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            await base.SchedulerStarted(cancellationToken).ConfigureAwait(false);
            await SetSchedulerState(SchedulerState.Started, cancellationToken).ConfigureAwait(false);
        }

        public override Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            return SetSchedulerState(SchedulerState.Paused, cancellationToken);
        }

        public override Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            return SetSchedulerState(SchedulerState.Resumed, cancellationToken);
        }

        public override async Task Shutdown(CancellationToken cancellationToken = default)
        {
            await base.Shutdown(cancellationToken).ConfigureAwait(false);

            await SetSchedulerState(SchedulerState.Shutdown, cancellationToken).ConfigureAwait(false);
            
            documentStore.Dispose();
            documentStore = null;
        }

        private async Task SetSchedulerState(SchedulerState state, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
                scheduler.State = state;
                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as
        /// appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        protected override Task RecoverJobs(CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                async conn =>
                {
                    try
                    {
                        Log.Info("Trying to recover persisted scheduler data for" + InstanceName);

                        // update inconsistent states
                        var queryResult = await conn.QueryTriggers()
                            .Where(x => x.State == InternalTriggerState.Acquired || x.State == InternalTriggerState.Blocked)
                            .ToListAsync(cancellationToken).ConfigureAwait(false);

                        foreach (var trigger in queryResult)
                        {
                            trigger.State = InternalTriggerState.Waiting;
                        }

                        Log.Info($"Freed {queryResult.Count} triggers from 'acquired' / 'blocked' state.");

                        // recover jobs marked for recovery that were not fully executed
                        List<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                        var queryResultJobs = await conn.QueryJobs()
                            .Where(x => x.RequestsRecovery)
                            .ToListAsync(cancellationToken).ConfigureAwait(false);

                        foreach (var job in queryResultJobs)
                        {
                            var triggers = await GetTriggersForJob(new JobKey(job.Name, job.Group), cancellationToken).ConfigureAwait(false);
                            recoveringJobTriggers.AddRange(triggers);
                        }

                        Log.Info($"Recovering {recoveringJobTriggers.Count} jobs that were in-progress at the time of the last shut-down.");

                        foreach (IOperableTrigger trigger in recoveringJobTriggers)
                        {
                            if (await CheckExists(trigger.JobKey, cancellationToken).ConfigureAwait(false))
                            {
                                trigger.ComputeFirstFireTimeUtc(null);
                                await StoreTrigger(trigger, true, cancellationToken).ConfigureAwait(false);
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
                                .ToListAsync(cancellationToken).ConfigureAwait(false);

                            foreach (var trigger in triggersInStateComplete)
                            {
                                await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group), cancellationToken).ConfigureAwait(false);
                            }
                        }

                        var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);
                        scheduler.State = SchedulerState.Started;
                    }
                    catch (JobPersistenceException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
                    }
                },
                cancellationToken);
        }

        protected override IClusterManagementOperations ClusterManagementOperations => this;

        protected override IMisfireHandlerOperations MisfireHandlerOperations => this;

        /// <inheritdoc />
        public async Task StoreJobAndTrigger(
            IJobDetail newJob,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            await ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                await StoreJob(conn, newJob, false, cancellationToken).ConfigureAwait(false);
                await StoreTrigger(conn, newTrigger, newJob, false, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => StoreJob(conn, newJob, replaceExisting, cancellationToken),
                cancellationToken);
        }

        private async Task StoreJob(
            RavenConnection conn,
            IJobDetail newJob, 
            bool replaceExisting, 
            CancellationToken cancellationToken)
        {
            var existingJob = await CheckExists(conn, newJob.Key, cancellationToken).ConfigureAwait(false);
            if (existingJob)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
            }

            var job = new Job(newJob, InstanceName);
            await conn.StoreAsync(job, job.Id, cancellationToken).ConfigureAwait(false);
        }

        public Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
            bool replace,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                // TODO: make this more efficient with a true bulk operation...
                foreach (IJobDetail job in triggersAndJobs.Keys)
                {
                    await StoreJob(conn, job, replace, cancellationToken).ConfigureAwait(false);
                    foreach (ITrigger trigger in triggersAndJobs[job])
                    {
                        await StoreTrigger(conn, (IOperableTrigger) trigger, job, replace, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
                    }
                }            
            }, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                if (!await CheckExists(jobKey, cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }

                session.Delete(jobKey.DocumentId(InstanceName));
                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
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
                result &= await RemoveJob(key, cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        /// <inheritdoc />
        public Task<IJobDetail> RetrieveJob(
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock(conn => RetrieveJob(conn, jobKey, cancellationToken), cancellationToken);
        }

        private static async Task<IJobDetail> RetrieveJob(
            RavenConnection conn,
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            var job = await conn.LoadJob(jobKey, cancellationToken).ConfigureAwait(false);
            return job?.Deserialize();
        }

        /// <inheritdoc />
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

        private async Task StoreTrigger(
            RavenConnection conn,
            IOperableTrigger newTrigger,
            IJobDetail job,
            bool replaceExisting,
            InternalTriggerState state,
            bool forceState,
            bool recovering,
            CancellationToken cancellationToken = default)
        {
            var existingTrigger = await CheckExists(newTrigger.Key, cancellationToken).ConfigureAwait(false);
            if (existingTrigger)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
            }
                    
            try
            {
                if (!forceState)
                {
                    var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);

                    bool shouldBePaused = scheduler.PausedTriggerGroups.Contains(newTrigger.Key.Group)
                                          || scheduler.PausedTriggerGroups.Contains(AllGroupsPaused);

                    if (shouldBePaused)
                    {
                        scheduler.PausedTriggerGroups.Add(newTrigger.Key.Group);
                    }

                    if (shouldBePaused && (state == InternalTriggerState.Waiting || state == InternalTriggerState.Acquired))
                    {
                        state = InternalTriggerState.Paused;
                    }
                }

                if (job == null)
                {
                    job = await RetrieveJob(conn, newTrigger.JobKey, cancellationToken).ConfigureAwait(false);
                }
                if (job == null)
                {
                    throw new JobPersistenceException($"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
                }
                if (job.ConcurrentExecutionDisallowed && !recovering)
                {
                    state = await CheckBlockedState(conn, job.Key, state, cancellationToken).ConfigureAwait(false);
                }
                
                // Overwrite if exists
                var trigger = new Trigger(newTrigger, InstanceName)
                {
                    State = state
                };
                await conn.StoreAsync(trigger, trigger.Id, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                string message = $"Couldn't store trigger '{newTrigger.Key}' for '{newTrigger.JobKey}' job: {e.Message}";
                throw new JobPersistenceException(message, e);
            }
        }
        
        /// <inheritdoc />
        public Task<bool> RemoveTrigger(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => RemoveTrigger(conn, triggerKey, cancellationToken),
                cancellationToken);
        }

        private async Task<bool> RemoveTrigger(
            RavenConnection conn, 
            TriggerKey triggerKey, 
            CancellationToken cancellationToken = default)
        {
            var trigger = await conn.LoadTrigger(triggerKey, cancellationToken).ConfigureAwait(false);

            if (trigger == null)
            {
                return false;
            }

            var job = await RetrieveJob(new JobKey(trigger.JobName, trigger.Group), cancellationToken).ConfigureAwait(false);

            // Delete trigger
            conn.Delete(trigger);

            // Remove the trigger's job if it is not associated with any other triggers
            var trigList = await GetTriggersForJob(job.Key, cancellationToken).ConfigureAwait(false);
            if ((trigList == null || trigList.Count == 0) && !job.Durable)
            {
                if (await RemoveJob(job.Key, cancellationToken).ConfigureAwait(false))
                {
                    await SchedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken).ConfigureAwait(false);
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
                result &= await RemoveTrigger(key, cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<bool> ReplaceTrigger(
            TriggerKey triggerKey,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            var wasRemoved = await RemoveTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
            if (wasRemoved)
            {
                await StoreTrigger(newTrigger, true, cancellationToken).ConfigureAwait(false);
            }

            return wasRemoved;
        }

        /// <inheritdoc />
        public Task<IOperableTrigger> RetrieveTrigger(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock(
                conn => RetrieveTrigger(conn, triggerKey, cancellationToken),
                cancellationToken);
        }

        private async Task<IOperableTrigger> RetrieveTrigger(
            RavenConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            var trigger = await conn.LoadTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
            return trigger?.Deserialize();
        }

        /// <inheritdoc />
        public async Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            bool answer;
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
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
        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock( // no locks necessary for read...
                conn => CheckExists(conn, jobKey, cancellationToken), cancellationToken);
        }

        /// <inheritdoc />
        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock( // no locks necessary for read...
                conn => CheckExists(conn, triggerKey, cancellationToken), cancellationToken);
        }
        
        private async Task<bool> CheckExists<T>(
            RavenConnection conn, 
            Key<T> jobKey, 
            CancellationToken cancellationToken = default)
        {
            return await conn.ExistsAsync(jobKey.DocumentId(InstanceName)).ConfigureAwait(false);
        }
        
        /// <inheritdoc />
        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);
                scheduler.Calendars.Clear();
                scheduler.PausedJobGroups.Clear();
                scheduler.PausedTriggerGroups.Clear();

                var options = new QueryOperationOptions
                {
                    AllowStale = false
                };
                var op = await documentStore.Operations.SendAsync(
                    new DeleteByQueryOperation<Trigger, TriggerIndex>(x => x.Scheduler == InstanceName, options),
                    token: cancellationToken).ConfigureAwait(false);

                op.WaitForCompletion();

                op = await documentStore.Operations.SendAsync(
                    new DeleteByQueryOperation<Job, JobIndex>(x => x.Scheduler == InstanceName, options),
                    token: cancellationToken).ConfigureAwait(false);

                op.WaitForCompletion();
                
                op = await documentStore.Operations.SendAsync(
                    new DeleteByQueryOperation<FiredTrigger, FiredTriggerIndex>(x => x.Scheduler == InstanceName, options),
                    token: cancellationToken).ConfigureAwait(false);

                op.WaitForCompletion();
                
            }, cancellationToken);
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
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);

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
                    .Select(x => x.Id)
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

                if (triggersKeysToUpdate.Count == 0)
                {
                    await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                foreach (var triggerKey in triggersKeysToUpdate)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(triggerKey, cancellationToken).ConfigureAwait(false);
                    var trigger = triggerToUpdate.Deserialize();
                    trigger.UpdateWithNewCalendar(calendarCopy, MisfireThreshold);
                    triggerToUpdate.UpdateFireTimes(trigger);

                }

                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            if (await RetrieveCalendar(calName, cancellationToken).ConfigureAwait(false) == null)
            {
                return false;
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
                scheduler.Calendars.Remove(calName);
                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            var callCollection = await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false);
            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        private async Task<IReadOnlyDictionary<string, ICalendar>> RetrieveCalendarCollection(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
                if (scheduler == null)
                {
                    throw new NullReferenceException($"Scheduler with instance name '{InstanceName}' is null");
                }

                if (scheduler.Calendars == null)
                {
                    throw new NullReferenceException($"Calendar collection in '{InstanceName}' is null");
                }

                return scheduler.Calendars;
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Query<Job, JobIndex>()
                    .CountAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                return await session.Query<Trigger, TriggerIndex>()
                    .CountAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false)).Count;
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
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

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
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

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
                    .ToListAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken)
        {
            return ExecuteWithoutLock(
                conn => GetTriggerGroupNames(conn, GroupMatcher<TriggerKey>.AnyGroup(), cancellationToken),
                cancellationToken);
        }

        private async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            RavenConnection conn,
            GroupMatcher<TriggerKey> matcher, 
            CancellationToken cancellationToken)
        {
            var query = conn.QueryTriggers();

            query = query.WhereMatches(matcher);
            
            var result = await query
                .Select(x => x.Group)
                .Distinct()
                .ToListAsync(cancellationToken).ConfigureAwait(false);

            return result;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false)).Keys.ToList();
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
                        .ToListAsync(cancellationToken).ConfigureAwait(false);

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
        public Task<TriggerState> GetTriggerState(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock(conn => GetTriggerState(conn, triggerKey, cancellationToken), cancellationToken);
        }

        private static async Task<TriggerState> GetTriggerState(
            RavenConnection conn,
            TriggerKey triggerKey, 
            CancellationToken cancellationToken = default)
        
        {
            var trigger = await conn.LoadTrigger(triggerKey, cancellationToken).ConfigureAwait(false);

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

        public Task PauseTrigger(
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess, 
                conn => PauseTrigger(conn, triggerKey, cancellationToken), cancellationToken);
   
        }

        private async Task PauseTrigger(
            RavenConnection conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            var trig = await conn.LoadTrigger(triggerKey, cancellationToken).ConfigureAwait(false);

            // if the trigger doesn't exist or is "complete" pausing it does not make sense...
            if (trig == null)
            {
                return;
            }

            if (trig.State == InternalTriggerState.Complete)
            {
                return;
            }

            trig.State = trig.State == InternalTriggerState.Blocked 
                ? InternalTriggerState.PausedAndBlocked 
                : InternalTriggerState.Paused;
            
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> PauseTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher, cancellationToken).ConfigureAwait(false);
            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
                pausedGroups.Add(triggerKey.Group);
            }

            return new HashSet<string>(pausedGroups);
        }

        /// <inheritdoc />
        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken).ConfigureAwait(false);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                await PauseTrigger(trigger.Key, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> PauseJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = await GetJobKeys(matcher, cancellationToken).ConfigureAwait(false);
            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey, cancellationToken).ConfigureAwait(false);
                pausedGroups.Add(jobKey.Group);

                using (var session = documentStore.OpenAsyncSession())
                {
                    var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
                    scheduler.PausedJobGroups.Add(matcher.CompareToValue);
                    await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            return pausedGroups;
        }

        /// <inheritdoc />
        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.TriggerAccess, async conn =>
            {
                if (!await CheckExists(conn, triggerKey, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                var trigger = await conn.LoadTrigger(triggerKey, cancellationToken).ConfigureAwait(false);

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                var blockedJobs = await GetBlockedJobs(conn, cancellationToken).ConfigureAwait(false);
                trigger.State = blockedJobs.Contains(trigger.JobKey) 
                    ? InternalTriggerState.Blocked 
                    : InternalTriggerState.Waiting;

                await ApplyMisfire(trigger).ConfigureAwait(false);
            }, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> ResumeTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetTriggerKeys(matcher, cancellationToken).ConfigureAwait(false);

            foreach (TriggerKey triggerKey in keys)
            {
                await ResumeTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        /// <inheritdoc />
        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            return ExecuteWithoutLock(connection => GetPausedTriggerGroups(connection, cancellationToken), cancellationToken);
        }

        async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(RavenConnection conn, CancellationToken cancellationToken)
        {
            var groups = await conn.QueryTriggers()
                .Where(x => x.State == InternalTriggerState.Paused || x.State == InternalTriggerState.PausedAndBlocked)
                .Select(x => x.Group)
                .Distinct()
                .ToListAsync(cancellationToken).ConfigureAwait(false);

            return groups;
        }

        /// <summary>
        /// Gets the paused job groups.
        /// </summary>
        private Task<HashSet<string>> GetPausedJobGroups(
            RavenConnection conn,
            CancellationToken cancellationToken = default)
        {
            return GetSchedulerData(conn, x => x.PausedJobGroups, cancellationToken);
        }

        /// <summary>
        /// Gets the blocked jobs set.
        /// </summary>
        private Task<HashSet<string>> GetBlockedJobs(
            RavenConnection conn,
            CancellationToken cancellationToken = default)
        {
            return GetSchedulerData(conn, x => x.BlockedJobs, cancellationToken);
        }
        
        private async Task<T> GetSchedulerData<T>(
            RavenConnection conn,
            Func<Scheduler, T> extractor, 
            CancellationToken cancellationToken = default)
        {
            var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);
            return extractor(scheduler);
        }

        /// <inheritdoc />
        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken).ConfigureAwait(false);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                await ResumeTrigger(trigger.Key, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyCollection<string>> ResumeJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetJobKeys(matcher, cancellationToken).ConfigureAwait(false);

            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);

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

                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }

            foreach (JobKey key in keys)
            {
                var triggers = await GetTriggersForJob(key, cancellationToken).ConfigureAwait(false);
                foreach (IOperableTrigger trigger in triggers)
                {
                    await ResumeTrigger(trigger.Key, cancellationToken).ConfigureAwait(false);
                }
            }

            return resumedGroups;
        }

        /// <inheritdoc />
        public async Task PauseAll(CancellationToken cancellationToken = default)
        {
            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken).ConfigureAwait(false);

            foreach (var groupName in triggerGroupNames)
            {
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task ResumeAll(CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken).ConfigureAwait(false);
                scheduler.PausedJobGroups.Clear();

                var triggerGroupNames = await GetTriggerGroupNames(cancellationToken).ConfigureAwait(false);

                foreach (var groupName in triggerGroupNames)
                {
                    await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="trigger">The trigger wrapper.</param>
        /// <returns></returns>
        private async Task<bool> ApplyMisfire(Trigger trigger)
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
                cal = await RetrieveCalendar(trigger.CalendarName).ConfigureAwait(false);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            await SchedulerSignaler.NotifyTriggerListenersMisfired(trig).ConfigureAwait(false);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await SchedulerSignaler.NotifySchedulerListenersFinalized(trig).ConfigureAwait(false);
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
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

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

                    if (await ApplyMisfire(candidateTrigger).ConfigureAwait(false))
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
                    Job job = await session.LoadAsync<Job>(candidateTrigger.JobKey, cancellationToken).ConfigureAwait(false);

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

                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        /// <inheritdoc />
        public Task ReleaseAcquiredTrigger(
            IOperableTrigger trigger,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => ReleaseAcquiredTrigger(conn, trigger, cancellationToken),
                cancellationToken);
        }

        private async Task ReleaseAcquiredTrigger(
            RavenConnection conn,
            IOperableTrigger trig, 
            CancellationToken cancellationToken = default)
        {
            var trigger = await conn.LoadTrigger(trig.Key, cancellationToken).ConfigureAwait(false);
            if (trigger == null || trigger.State != InternalTriggerState.Acquired)
            {
                return;
            }

            trigger.State = InternalTriggerState.Waiting;
        }

        /// <inheritdoc />
        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock<IReadOnlyCollection<TriggerFiredResult>>(
                LockType.TriggerAccess,
                async conn =>
                {
                    var results = new List<TriggerFiredResult>();
                    foreach (IOperableTrigger tr in triggers)
                    {
                        // was the trigger deleted since being acquired?
                        var trigger = await conn.LoadTrigger(tr.Key, cancellationToken).ConfigureAwait(false);

                        // was the trigger completed, paused, blocked, etc. since being acquired?
                        if (trigger?.State != InternalTriggerState.Acquired)
                        {
                            continue;
                        }

                        ICalendar cal = null;
                        if (trigger.CalendarName != null)
                        {
                            cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken).ConfigureAwait(false);
                            if (cal == null)
                            {
                                continue;
                            }
                        }

                        DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;

                        var trig = trigger.Deserialize();
                        trig.Triggered(cal);

                        TriggerFiredBundle bundle = new TriggerFiredBundle(
                            await RetrieveJob(trig.JobKey, cancellationToken).ConfigureAwait(false),
                            trig,
                            cal,
                            jobIsRecovering: false,
                            SystemTime.UtcNow(),
                            trig.GetPreviousFireTimeUtc(),
                            prevFireTime,
                            trig.GetNextFireTimeUtc());

                        IJobDetail job = bundle.JobDetail;

                        trigger.UpdateFireTimes(trig);
                        trigger.State = InternalTriggerState.Waiting;

                        if (job.ConcurrentExecutionDisallowed)
                        {
                            var dbTriggers = await conn.QueryTriggers()
                                .Where(x => x.Group == job.Key.Group && x.JobName == job.Key.Name)
                                .ToListAsync(cancellationToken).ConfigureAwait(false);

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

                            var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);
                            scheduler.BlockedJobs.Add(job.Key.DocumentId(InstanceName));
                        }

                        results.Add(new TriggerFiredResult(bundle));
                    }

                    return results;
                }, cancellationToken);
        }

        /// <inheritdoc />
        public virtual Task TriggeredJobComplete(
            IOperableTrigger trigger,
            IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(
                LockType.TriggerAccess,
                conn => TriggeredJobComplete(conn, trigger, jobDetail, triggerInstCode, cancellationToken),
                cancellationToken);
        }

        private async Task TriggeredJobComplete(
            RavenConnection conn,
            IOperableTrigger trig,
            IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken)
        {
            var trigger = await conn.LoadTrigger(trig.Key, cancellationToken).ConfigureAwait(false);
            var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);

            // It's possible that the job or trigger is null if it was deleted during execution
            var job = await conn.LoadJob(trig.JobKey, cancellationToken).ConfigureAwait(false);

            if (job != null)
            {
                if (jobDetail.PersistJobDataAfterExecution)
                {
                    job.JobDataMap = jobDetail.JobDataMap;
                }

                if (job.ConcurrentExecutionDisallowed)
                {
                    scheduler.BlockedJobs.Remove(job.Id);

                    var triggers = await conn.QueryTriggers()
                        .Where(x => x.Group == job.Group && x.JobName == job.Name)
                        .ToListAsync(cancellationToken).ConfigureAwait(false);

                    foreach (Trigger triggerToUpdate in triggers)
                    {
                        if (triggerToUpdate.State == InternalTriggerState.Blocked)
                        {
                            triggerToUpdate.State = InternalTriggerState.Waiting;
                        }

                        if (triggerToUpdate.State == InternalTriggerState.PausedAndBlocked)
                        {
                            triggerToUpdate.State = InternalTriggerState.Paused;
                        }
                    }

                    await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                // even if it was deleted, there may be cleanup to do
                scheduler.BlockedJobs.Remove(jobDetail.Key.DocumentId(InstanceName));
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
                            await RemoveTrigger(trig.Key, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            //Deleting cancelled - trigger still active
                        }
                    }
                    else
                    {
                        await RemoveTrigger(trig.Key, cancellationToken).ConfigureAwait(false);
                        await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    trigger.State = InternalTriggerState.Complete;
                    await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    trigger.State = InternalTriggerState.Error;
                    await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Error, cancellationToken).ConfigureAwait(false);
                    await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Complete, cancellationToken).ConfigureAwait(false);
                    await SchedulerSignaler.SignalSchedulingChange(null, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Sets the State of all triggers of job to specified State.
        /// </summary>
        private async Task SetAllTriggersOfJobToState(
            JobKey jobKey,
            InternalTriggerState state,
            CancellationToken cancellationToken = default)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                var triggers = await session.Query<Trigger, TriggerIndex>()
                    .Where(x => x.Group == jobKey.Group && x.JobName == jobKey.Name)
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

                foreach (var triggerToUpdate in triggers)
                {
                    triggerToUpdate.State = state;
                }

                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        
        /// <summary>
        /// Determines if a Trigger for the given job should be blocked.
        /// State can only transition to StatePausedBlocked/StateBlocked from
        /// StatePaused/StateWaiting respectively.
        /// </summary>
        /// <returns>StatePausedBlocked, StateBlocked, or the currentState. </returns>
        private async Task<InternalTriggerState> CheckBlockedState(
            RavenConnection conn,
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
                var firedTriggers = await conn.QueryFiredTriggers()
                    .Where(x => x.JobGroup == jobKey.Group && x.JobName == jobKey.Group)
                    .Take(1)
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

                if (firedTriggers.Count > 0)
                {
                    FiredTrigger rec = firedTriggers.First();
                    if (rec.State != InternalTriggerState.Acquired && rec.IsNonConcurrent)
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

        /// <summary>
        /// Pause all of the <see cref="ITrigger" />s in the given group.
        /// </summary>
        private async Task<IReadOnlyCollection<string>> PauseTriggerGroup(
            RavenConnection conn,
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var triggers = await conn.QueryTriggers()
                    .WhereMatches(matcher)
                    .ToListAsync(cancellationToken).ConfigureAwait(false);

                var groups = new HashSet<string>();
                foreach (var trigger in triggers)
                {
                    groups.Add(trigger.Group);
                    switch (trigger.State)
                    {
                        case InternalTriggerState.Waiting:
                        case InternalTriggerState.Acquired:
                            trigger.State = InternalTriggerState.Paused;
                            break;
                        case InternalTriggerState.Blocked:
                            trigger.State = InternalTriggerState.PausedAndBlocked;
                            break;
                    }
                }

                // make sure to account for an exact group match for a group that doesn't yet exist
                StringOperator op = matcher.CompareWithOperator;
                if (op.Equals(StringOperator.Equality) && !groups.Contains(matcher.CompareToValue))
                {
                    groups.Add(matcher.CompareToValue);
                }

                var scheduler = await conn.LoadScheduler(cancellationToken).ConfigureAwait(false);
                foreach (string group in groups)
                {
                    scheduler.PausedTriggerGroups.Add(group);
                }

                return new ReadOnlyCompatibleHashSet<string>(groups);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Couldn't pause trigger group '{matcher}': {e.Message}", e);
            }
        }

        private Task<T> ExecuteWithoutLock<T>(
            Func<RavenConnection, Task<T>> txCallback,
            CancellationToken cancellationToken = default)
        {
            return ExecuteInLock(LockType.None, txCallback, cancellationToken);
        }

        private Task ExecuteInLock(
            LockType lockType,
            Func<RavenConnection, Task> txCallback,
            CancellationToken cancellationToken)
        {
            return ExecuteInLock<object>(lockType, async (conn) =>
            {
                await txCallback(conn).ConfigureAwait(false);
                return null;
            }, cancellationToken);
        }

        private async Task<T> ExecuteInLock<T>(
            LockType lockType,
            Func<RavenConnection, Task<T>> txCallback, 
            CancellationToken cancellationToken)
        {
            bool transOwner = false;
            Guid requestorId = Guid.NewGuid();
            RavenConnection conn = null;
            try
            {
                conn = new RavenConnection(documentStore.OpenAsyncSession(), InstanceName);
                if (lockType != LockType.None)
                {
                    transOwner = await lockHandler.ObtainLock(requestorId, conn, lockType, cancellationToken).ConfigureAwait(false);
                }

                T result = await txCallback(conn).ConfigureAwait(false);
                
                await conn.Commit(cancellationToken).ConfigureAwait(false);
                
                DateTimeOffset? sigTime = conn.SignalSchedulingChangeOnTxCompletion;
                if (sigTime != null)
                {
                    await SchedulerSignaler.SignalSchedulingChange(sigTime, CancellationToken.None).ConfigureAwait(false);
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
                if (transOwner)
                {
                    try
                    {
                        if (lockType != LockType.None)
                        {
                            await lockHandler.ReleaseLock(requestorId, lockType, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        conn?.Dispose();
                    }
                }
            }
        }

        public Task<bool> CheckCluster(Guid requestorId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        Task<RecoverMisfiredJobsResult> IMisfireHandlerOperations.RecoverMisfires(
            Guid requestorId,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}