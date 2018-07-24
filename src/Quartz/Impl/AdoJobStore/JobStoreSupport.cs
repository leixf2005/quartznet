#region License

/*
 * All content copyright Marko Lahma, unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Impl.AdoJobStore.Common;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Logging;
using Quartz.Simpl;
using Quartz.Spi;
using Quartz.Util;

namespace Quartz.Impl.AdoJobStore
{
    /// <summary>
    /// Contains base functionality for ADO.NET-based JobStore implementations.
    /// </summary>
    /// <author><a href="mailto:jeff@binaryfeed.org">Jeffrey Wescott</a></author>
    /// <author>James House</author>
    /// <author>Marko Lahma (.NET)</author>
    public abstract class JobStoreSupport : PersistentJobStore<ConnectionAndTransactionHolder>, IClusterManagementOperations, IMisfireHandlerOperations
    {
        protected const string LockTriggerAccess = "TRIGGER_ACCESS";
        protected const string LockStateAccess = "STATE_ACCESS";

        private string tablePrefix = AdoConstants.DefaultTablePrefix;
        private bool useProperties;
        protected Type delegateType;
        protected readonly Dictionary<string, ICalendar> calendarCache = new Dictionary<string, ICalendar>();
        private IDriverDelegate driverDelegate;
        private IsolationLevel isolationLevel = IsolationLevel.ReadCommitted;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobStoreSupport"/> class.
        /// </summary>
        protected JobStoreSupport()
        {
            delegateType = typeof (StdAdoDelegate);
        }

        /// <summary>
        /// Get or set the datasource name.
        /// </summary>
        public string DataSource { get; set; }

        /// <summary>
        /// Get or set the database connection manager.
        /// </summary>
        public IDbConnectionManager ConnectionManager { get; set; } = DBConnectionManager.Instance;

        /// <summary>
        /// Get or sets the prefix that should be pre-pended to all table names.
        /// </summary>
        public string TablePrefix
        {
            get => tablePrefix;
            set
            {
                if (value == null)
                {
                    value = "";
                }

                tablePrefix = value;
            }
        }

        /// <summary>
        /// Set whether string-only properties will be handled in JobDataMaps.
        /// </summary>
        public string UseProperties
        {
            set
            {
                if (value == null)
                {
                    value = "false";
                }

                useProperties = bool.Parse(value);
            }
        }

        public IObjectSerializer ObjectSerializer { get; set; }

        /// <inheritdoc />
        public override long EstimatedTimeToReleaseAndAcquireTrigger { get; } = 70;

        /// <summary>
        /// Gets or sets the database retry interval, alias for <see cref="DbRetryInterval"/>.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan DbRetryInterval
        {
            get => RetryInterval;
            set => RetryInterval = value;
        }

        /// <summary>
        /// Get or set whether this instance should use database-based thread
        /// synchronization.
        /// </summary>
        public bool UseDBLocks { get; set; }

        /// <summary>
        /// Set the transaction isolation level of DB connections to sequential.
        /// </summary>
        public bool TxIsolationLevelSerializable
        {
            get => isolationLevel == IsolationLevel.Serializable;
            set => isolationLevel = value ? IsolationLevel.Serializable : IsolationLevel.ReadCommitted;
        }

        /// <summary>
        /// Whether or not the query and update to acquire a Trigger for firing
        /// should be performed after obtaining an explicit DB lock (to avoid
        /// possible race conditions on the trigger's db row).  This is
        /// is considered unnecessary for most databases (due to the nature of
        ///  the SQL update that is performed), and therefore a superfluous performance hit.
        /// </summary>
        /// <remarks>
        /// However, if batch acquisition is used, it is important for this behavior
        /// to be used for all dbs.
        /// </remarks>
        public bool AcquireTriggersWithinLock { get; set; }

        /// <summary>
        /// Get or set the ADO.NET driver delegate class name.
        /// </summary>
        public virtual string DriverDelegateType { get; set; }

        /// <summary>
        /// The driver delegate's initialization string.
        /// </summary>
        public string DriverDelegateInitString { get; set; }

        /// <summary>
        /// set the SQL statement to use to select and lock a row in the "locks"
        /// table.
        /// </summary>
        /// <seealso cref="StdRowLockSemaphore" />
        public virtual string SelectWithLockSQL { get; set; }

        protected DbMetadata DbMetadata => ConnectionManager.GetDbMetadata(DataSource);

        protected abstract ConnectionAndTransactionHolder GetNonManagedTXConnection();

        /// <summary>
        /// Gets the connection and starts a new transaction.
        /// </summary>
        /// <returns></returns>
        protected virtual ConnectionAndTransactionHolder GetConnection()
        {
            DbConnection conn;
            DbTransaction tx;
            try
            {
                conn = ConnectionManager.GetConnection(DataSource);
                conn.Open();
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Failed to obtain DB connection from data source '{DataSource}': {e}", e);
            }
            if (conn == null)
            {
                throw new JobPersistenceException($"Could not get connection from DataSource '{DataSource}'");
            }

            try
            {
                tx = conn.BeginTransaction(isolationLevel);
            }
            catch (Exception e)
            {
                conn.Close();
                throw new JobPersistenceException("Failure setting up connection.", e);
            }

            return new ConnectionAndTransactionHolder(conn, tx);
        }

        /// <summary>
        /// Get the driver delegate for DB operations.
        /// </summary>
        protected virtual IDriverDelegate Delegate
        {
            get
            {
                lock (this)
                {
                    if (driverDelegate == null)
                    {
                        try
                        {
                            if (!string.IsNullOrWhiteSpace(DriverDelegateType))
                            {
                                delegateType = TypeLoadHelper.LoadType(DriverDelegateType);
                            }

                            IDbProvider dbProvider = ConnectionManager.GetDbProvider(DataSource);
                            var args = new DelegateInitializationArgs();
                            args.UseProperties = CanUseProperties;
                            args.TablePrefix = tablePrefix;
                            args.InstanceName = InstanceName;
                            args.InstanceId = InstanceId;
                            args.DbProvider = dbProvider;
                            args.TypeLoadHelper = TypeLoadHelper;
                            args.ObjectSerializer = ObjectSerializer;
                            args.InitString = DriverDelegateInitString;

                            ConstructorInfo ctor = delegateType.GetConstructor(new Type[0]);
                            if (ctor == null)
                            {
                                throw new InvalidConfigurationException("Configured delegate does not have public constructor that takes no arguments");
                            }

                            driverDelegate = (IDriverDelegate) ctor.Invoke(null);
                            driverDelegate.Initialize(args);
                        }
                        catch (Exception e)
                        {
                            throw new NoSuchDelegateException("Couldn't instantiate delegate: " + e.Message, e);
                        }
                    }
                }
                return driverDelegate;
            }
        }

        private IDbProvider DbProvider => ConnectionManager.GetDbProvider(DataSource);

        protected internal virtual ISemaphore LockHandler { get; set; }

        /// <summary>
        /// Get whether String-only properties will be handled in JobDataMaps.
        /// </summary>
        public virtual bool CanUseProperties => useProperties;

        public override Task Initialize(
            ITypeLoadHelper loadHelper,
            ISchedulerSignaler schedulerSignaler,
            CancellationToken cancellationToken = default)
        {
            base.Initialize(loadHelper, schedulerSignaler, cancellationToken);

            if (string.IsNullOrWhiteSpace(DataSource))
            {
                throw new SchedulerConfigException("DataSource name not set.");
            }

            if (Delegate is SQLiteDelegate && (LockHandler == null || LockHandler.GetType() != typeof(UpdateLockRowSemaphore)))
            {
                Log.Info("Detected SQLite usage, changing to use UpdateLockRowSemaphore");
                var lockHandler = new UpdateLockRowSemaphore(DbProvider)
                {
                    SchedName = InstanceName,
                    TablePrefix = TablePrefix
                };
                LockHandler = lockHandler;
            }

            if (Delegate is SQLiteDelegate)
            {
                if (Clustered)
                {
                    throw new InvalidConfigurationException("SQLite cannot be used as clustered mode due to locking problems");
                }
                if (!AcquireTriggersWithinLock)
                {
                    Log.Info("With SQLite we need to set AcquireTriggersWithinLock to true, changing");
                    AcquireTriggersWithinLock = true;

                }
                if (!TxIsolationLevelSerializable)
                {
                    Log.Info("Detected usage of SQLiteDelegate - defaulting 'txIsolationLevelSerializable' to 'true'");
                    TxIsolationLevelSerializable = true;
                }
            }

            // If the user hasn't specified an explicit lock handler, then
            // choose one based on CMT/Clustered/UseDBLocks.
            if (LockHandler == null)
            {
                // If the user hasn't specified an explicit lock handler,
                // then we *must* use DB locks with clustering
                if (Clustered)
                {
                    UseDBLocks = true;
                }

                if (UseDBLocks)
                {
                    if (Delegate is SqlServerDelegate)
                    {
                        if (SelectWithLockSQL == null)
                        {
                            const string DefaultLockSql = "SELECT * FROM {0}LOCKS WITH (UPDLOCK,ROWLOCK) WHERE " + AdoConstants.ColumnSchedulerName + " = {1} AND LOCK_NAME = @lockName";
                            Log.InfoFormat("Detected usage of SqlServerDelegate - defaulting 'selectWithLockSQL' to '" + DefaultLockSql + "'.", TablePrefix, "'" + InstanceName + "'");
                            SelectWithLockSQL = DefaultLockSql;
                        }
                    }

                    Log.Info("Using db table-based data access locking (synchronization).");
                    LockHandler = new StdRowLockSemaphore(TablePrefix, InstanceName, SelectWithLockSQL, DbProvider);
                }
                else
                {
                    Log.Info("Using thread monitor-based data access locking (synchronization).");
                    LockHandler = new SimpleSemaphore();
                }
            }
            else
            {
                // be ready to give a friendly warning if SQL Server is used and sub-optimal locking
                if (LockHandler is UpdateLockRowSemaphore && Delegate is SqlServerDelegate)
                {
                    Log.Warn("Detected usage of SqlServerDelegate and UpdateLockRowSemaphore, removing 'quartz.jobStore.lockHandler.type' would allow more efficient SQL Server specific (UPDLOCK,ROWLOCK) row access");
                }
                // be ready to give a friendly warning if SQL Server provider and wrong delegate
                if (DbProvider != null && DbProvider.Metadata.ConnectionType == typeof (SqlConnection) && !(Delegate is SqlServerDelegate))
                {
                    Log.Warn("Detected usage of SQL Server provider without SqlServerDelegate, SqlServerDelegate would provide better performance");
                }
            }

            return TaskUtil.CompletedTask;
        }
        
        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// it should free up all of it's resources because the scheduler is
        /// shutting down.
        /// </summary>
        public override async Task Shutdown(CancellationToken cancellationToken = default)
        {
            await base.Shutdown(cancellationToken).ConfigureAwait(false);
            try
            {
                ConnectionManager.Shutdown(DataSource);
            }
            catch (Exception sqle)
            {
                Log.WarnException("Database connection Shutdown unsuccessful.", sqle);
            }
        }

        protected virtual async Task ReleaseLock(
            Guid requestorId,
            LockType lockType,
            CancellationToken cancellationToken)
        {
            try
            {
                await LockHandler.ReleaseLock(requestorId, GetLockName(lockType), cancellationToken).ConfigureAwait(false);
            }
            catch (LockException le)
            {
                Log.ErrorException("Error returning lock: " + le.Message, le);
            }
        }

        protected override Task RecoverJobs(CancellationToken cancellationToken)
        {
            return ExecuteInNonManagedTXLock(
                LockType.TriggerAccess, 
                conn => RecoverJobs(conn, cancellationToken),
                cancellationToken);
        }

        protected virtual async Task RecoverJobs(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // update inconsistent job states
                int rows = await Delegate.UpdateTriggerStatesFromOtherStates(
                    conn, 
                    newState: AdoConstants.StateWaiting, 
                    oldState1: AdoConstants.StateAcquired, 
                    oldState2: AdoConstants.StateBlocked,
                    cancellationToken).ConfigureAwait(false);

                rows += await Delegate.UpdateTriggerStatesFromOtherStates(
                    conn, 
                    newState: AdoConstants.StatePaused,
                    oldState1: AdoConstants.StatePausedBlocked,
                    oldState2: AdoConstants.StatePausedBlocked, 
                    cancellationToken).ConfigureAwait(false);

                Log.Info($"Freed {rows} triggers from 'acquired' / 'blocked' state.");

                // clean up misfired jobs
                await RecoverMisfiredJobs(conn, true, cancellationToken).ConfigureAwait(false);

                // recover jobs marked for recovery that were not fully executed
                var recoveringJobTriggers = await Delegate.SelectTriggersForRecoveringJobs(conn, cancellationToken).ConfigureAwait(false);
                Log.Info($"Recovering {recoveringJobTriggers.Count} jobs that were in-progress at the time of the last shut-down.");

                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (await JobExists(conn, trigger.JobKey, cancellationToken).ConfigureAwait(false))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(conn, trigger, null, false, InternalTriggerState.Waiting, false, true, cancellationToken).ConfigureAwait(false);
                    }
                }
                Log.Info("Recovery complete.");

                // remove lingering 'complete' triggers...
                var triggersInState = await Delegate.SelectTriggersInState(conn, AdoConstants.StateComplete, cancellationToken).ConfigureAwait(false);
                foreach (var trigger in triggersInState)
                {
                    await RemoveTrigger(conn, trigger, cancellationToken).ConfigureAwait(false);
                }
                Log.Info($"Removed {triggersInState.Count} 'complete' triggers.");

                // clean up any fired trigger entries
                int n = await Delegate.DeleteFiredTriggers(conn, cancellationToken).ConfigureAwait(false);
                Log.Info("Removed " + n + " stale fired job entries.");
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

        protected override Task<bool> GetMisfiredTriggersInWaitingState(
            ConnectionAndTransactionHolder conn,
            int count,
            List<TriggerKey> resultList,
            CancellationToken cancellationToken)
        {
            return Delegate.HasMisfiredTriggersInState(conn, AdoConstants.StateWaiting, MisfireTime, count, resultList, cancellationToken);
        }

        protected override async Task StoreJob(
            ConnectionAndTransactionHolder conn,
            IJobDetail newJob,
            bool replaceExisting,
            CancellationToken cancellationToken)
        {
            bool existingJob = await JobExists(conn, newJob.Key, cancellationToken).ConfigureAwait(false);
            if (existingJob)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }

                await Delegate.UpdateJobDetail(conn, newJob, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await Delegate.InsertJobDetail(conn, newJob, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Check existence of a given job.
        /// </summary>
        protected virtual async Task<bool> JobExists(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await Delegate.JobExists(conn, jobKey, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException(
                    "Couldn't determine job existence (" + jobKey + "): " + e.Message, e);
            }
        }

        /// <summary>
        /// Insert or update a trigger.
        /// </summary>
        protected override async Task StoreTrigger(
            ConnectionAndTransactionHolder conn,
            IOperableTrigger newTrigger,
            IJobDetail job,
            bool replaceExisting,
            InternalTriggerState state,
            bool forceState,
            bool recovering,
            CancellationToken cancellationToken = default)
        {
            bool existingTrigger = await TriggerExists(conn, newTrigger.Key, cancellationToken).ConfigureAwait(false);

            if (existingTrigger && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            try
            {
                if (!forceState)
                {
                    bool shouldBePaused = await Delegate.IsTriggerGroupPaused(conn, newTrigger.Key.Group, cancellationToken).ConfigureAwait(false);

                    if (!shouldBePaused)
                    {
                        shouldBePaused = await Delegate.IsTriggerGroupPaused(conn, AdoConstants.AllGroupsPaused, cancellationToken).ConfigureAwait(false);

                        if (shouldBePaused)
                        {
                            await Delegate.InsertPausedTriggerGroup(conn, newTrigger.Key.Group, cancellationToken).ConfigureAwait(false);
                        }
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
                if (existingTrigger)
                {
                    await Delegate.UpdateTrigger(conn, newTrigger, GetStateName(state), job, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await Delegate.InsertTrigger(conn, newTrigger, GetStateName(state), job, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                string message = $"Couldn't store trigger '{newTrigger.Key}' for '{newTrigger.JobKey}' job: {e.Message}";
                throw new JobPersistenceException(message, e);
            }
        }

        /// <summary>
        /// Check existence of a given trigger.
        /// </summary>
        protected virtual async Task<bool> TriggerExists(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            try
            {
                return await Delegate.TriggerExists(conn, triggerKey, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException(
                    "Couldn't determine trigger existence (" + triggerKey + "): " + e.Message, e);
            }
        }

        protected override Task<IReadOnlyCollection<FiredTriggerRecord>> SelectFiredTriggerRecordsByJob(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey, 
            CancellationToken cancellationToken)
        {
            return Delegate.SelectFiredTriggerRecordsByJob(conn, jobKey.Name, jobKey.Group, cancellationToken);
        }

        /// <summary>
        /// Delete a job and its listeners.
        /// </summary>
        /// <seealso cref="RemoveTrigger(ConnectionAndTransactionHolder, TriggerKey, IJobDetail, CancellationToken)" />
        protected override async Task<bool> DeleteJobAndChildren(
            ConnectionAndTransactionHolder conn,
            JobKey key,
            CancellationToken cancellationToken)
        {
            return await Delegate.DeleteJobDetail(conn, key, cancellationToken).ConfigureAwait(false) > 0;
        }

        /// <summary>
        /// Delete a trigger, its listeners, and its Simple/Cron/BLOB sub-table entry.
        /// </summary>
        /// <seealso cref="RemoveTrigger(ConnectionAndTransactionHolder, TriggerKey, IJobDetail, CancellationToken)" />
        protected override async Task<bool> DeleteTriggerAndChildren(
            ConnectionAndTransactionHolder conn,
            TriggerKey key,
            CancellationToken cancellationToken)
        {
            return await Delegate.DeleteTrigger(conn, key, cancellationToken).ConfigureAwait(false) > 0;
        }

        protected override Task<IJobDetail> RetrieveJob(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            return Delegate.SelectJobDetail(conn, jobKey, TypeLoadHelper, cancellationToken);
        }

        protected override async Task<bool> RemoveTrigger(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            IJobDetail job,
            CancellationToken cancellationToken = default)
        {
            bool removedTrigger;
            try
            {
                // this must be called before we delete the trigger, obviously
                // we use fault tolerant type loading as we only want to delete things
                if (job == null)
                {
                    job = await Delegate.SelectJobForTrigger(conn, triggerKey, new NullJobTypeLoader(), false, cancellationToken).ConfigureAwait(false);
                }

                removedTrigger = await DeleteTriggerAndChildren(conn, triggerKey, cancellationToken).ConfigureAwait(false);

                if (null != job && !job.Durable)
                {
                    int numTriggers = await Delegate.SelectNumTriggersForJob(conn, job.Key, cancellationToken).ConfigureAwait(false);
                    if (numTriggers == 0)
                    {
                        // Don't call RemoveJob() because we don't want to check for
                        // triggers again.
                        if (await DeleteJobAndChildren(conn, job.Key, cancellationToken).ConfigureAwait(false))
                        {
                            await SchedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't remove trigger: " + e.Message, e);
            }

            return removedTrigger;
        }

        private class NullJobTypeLoader : ITypeLoadHelper
        {
            public void Initialize()
            {
            }

            public Type LoadType(string name)
            {
                return null;
            }
        }

        protected override Task<IJobDetail> GetJobForTrigger(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectJobForTrigger(conn, triggerKey, TypeLoadHelper, cancellationToken);
        }

        protected override Task<IOperableTrigger> RetrieveTrigger(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTrigger(conn, triggerKey, cancellationToken);
        }

        protected override async Task<TriggerState> GetTriggerState(ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            string ts = await Delegate.SelectTriggerState(conn, triggerKey, cancellationToken).ConfigureAwait(false);

            switch (ts)
            {
                case null:
                case AdoConstants.StateDeleted:
                    return TriggerState.None;
                case AdoConstants.StateComplete:
                    return TriggerState.Complete;
                case AdoConstants.StatePaused:
                case AdoConstants.StatePausedBlocked:
                    return TriggerState.Paused;
                case AdoConstants.StateError:
                    return TriggerState.Error;
                case AdoConstants.StateBlocked:
                    return TriggerState.Blocked;
            }

            return TriggerState.Normal;
        }

        protected override async Task StoreCalendar(
            ConnectionAndTransactionHolder conn,
            string name,
            ICalendar calendar, 
            bool replaceExisting, 
            bool updateTriggers, 
            CancellationToken cancellationToken)
        {
            bool existingCal = await CalendarExists(conn, name, cancellationToken).ConfigureAwait(false);
            if (existingCal && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException("Calendar with name '" + name + "' already exists.");
            }

            if (existingCal)
            {
                if (await Delegate.UpdateCalendar(conn, name, calendar, cancellationToken).ConfigureAwait(false) < 1)
                {
                    throw new JobPersistenceException("Couldn't store calendar.  Update failed.");
                }

                if (updateTriggers)
                {
                    var triggers = await Delegate.SelectTriggersForCalendar(conn, name, cancellationToken).ConfigureAwait(false);

                    foreach (IOperableTrigger trigger in triggers)
                    {
                        trigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        await StoreTrigger(conn, trigger, null, true, InternalTriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                if (await Delegate.InsertCalendar(conn, name, calendar, cancellationToken).ConfigureAwait(false) < 1)
                {
                    throw new JobPersistenceException("Couldn't store calendar.  Insert failed.");
                }
            }

            if (!Clustered)
            {
                calendarCache[name] = calendar; // lazy-cache
            }
        }

        protected override async Task<bool> RemoveCalendar(
            ConnectionAndTransactionHolder conn,
            string calendarName,
            CancellationToken cancellationToken = default)
        {
            if (await Delegate.CalendarIsReferenced(conn, calendarName, cancellationToken).ConfigureAwait(false))
            {
                throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
            }

            if (!Clustered)
            {
                calendarCache.Remove(calendarName);
            }

            return await Delegate.DeleteCalendar(conn, calendarName, cancellationToken).ConfigureAwait(false) > 0;
        }

        protected override async Task<ICalendar> RetrieveCalendar(
            ConnectionAndTransactionHolder conn,
            string calName,
            CancellationToken cancellationToken)
        {
            // all calendars are persistent, but we lazy-cache them during run
            // time as long as we aren't running clustered.
            ICalendar calendar = null;
            if (!Clustered)
            {
                calendarCache.TryGetValue(calName, out calendar);
            }
            if (calendar != null)
            {
                return calendar;
            }

            calendar = await Delegate.SelectCalendar(conn, calName, cancellationToken).ConfigureAwait(false);
            if (!Clustered)
            {
                calendarCache[calName] = calendar; // lazy-cache...
            }

            return calendar;
        }

        protected override Task<int> GetNumberOfJobs(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            return Delegate.SelectNumJobs(conn, cancellationToken);
        }

        protected override Task<int> GetNumberOfTriggers(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            return Delegate.SelectNumTriggers(conn, cancellationToken);
        }

        protected override Task<int> GetNumberOfCalendars(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            return Delegate.SelectNumCalendars(conn, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<JobKey>> GetJobKeys(
            ConnectionAndTransactionHolder conn,
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            return Delegate.SelectJobsInGroup(conn, matcher, cancellationToken);
        }

        protected override Task<bool> CalendarExists(
            ConnectionAndTransactionHolder conn,
            string calName,
            CancellationToken cancellationToken = default)
        {
            return Delegate.CalendarExists(conn, calName, cancellationToken);
        }

        protected override Task<bool> CheckExists(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            CancellationToken cancellationToken)
        {
            return Delegate.JobExists(conn, jobKey, cancellationToken);
        }

        protected override Task<bool> CheckExists(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            return Delegate.TriggerExists(conn, triggerKey, cancellationToken);
        }

        protected override Task ClearAllSchedulingData(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.ClearData(conn, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
            ConnectionAndTransactionHolder conn,
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTriggersInGroup(conn, matcher, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<string>> GetJobGroupNames(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectJobGroups(conn, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTriggerGroups(conn, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<string>> GetCalendarNames(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectCalendars(conn, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTriggersForJob(conn, jobKey, cancellationToken);
        }

        protected override Task<IReadOnlyCollection<TriggerKey>> GetTriggerNamesForJob(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTriggerNamesForJob(conn, jobKey, cancellationToken);
        }

        protected override async Task PauseTrigger(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            string oldState = await Delegate.SelectTriggerState(conn, triggerKey, cancellationToken).ConfigureAwait(false);

            if (oldState == AdoConstants.StateWaiting || oldState== AdoConstants.StateAcquired)
            {
                await Delegate.UpdateTriggerState(conn, triggerKey, AdoConstants.StatePaused, cancellationToken).ConfigureAwait(false);
            }
            else if (oldState.Equals(AdoConstants.StateBlocked))
            {
                await Delegate.UpdateTriggerState(conn, triggerKey, AdoConstants.StatePausedBlocked, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override async Task<IReadOnlyCollection<string>> PauseTriggerGroup(
            ConnectionAndTransactionHolder conn,
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            try
            {
                await Delegate.UpdateTriggerGroupStateFromOtherStates(
                    conn, 
                    matcher, 
                    newState: AdoConstants.StatePaused,
                    oldState1: AdoConstants.StateAcquired, 
                    oldState2: AdoConstants.StateWaiting,
                    oldState3: AdoConstants.StateWaiting, 
                    cancellationToken).ConfigureAwait(false);

                await Delegate.UpdateTriggerGroupStateFromOtherState(
                    conn, 
                    matcher,
                    newState: AdoConstants.StatePausedBlocked,
                    oldState: AdoConstants.StateBlocked, 
                    cancellationToken).ConfigureAwait(false);

                var groups = new List<string>(await Delegate.SelectTriggerGroups(conn, matcher, cancellationToken).ConfigureAwait(false));

                // make sure to account for an exact group match for a group that doesn't yet exist
                StringOperator op = matcher.CompareWithOperator;
                if (op.Equals(StringOperator.Equality) && !groups.Contains(matcher.CompareToValue))
                {
                    groups.Add(matcher.CompareToValue);
                }

                foreach (string group in groups)
                {
                    if (!await Delegate.IsTriggerGroupPaused(conn, group, cancellationToken).ConfigureAwait(false))
                    {
                        await Delegate.InsertPausedTriggerGroup(conn, group, cancellationToken).ConfigureAwait(false);
                    }
                }

                return new ReadOnlyCompatibleHashSet<string>(groups);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't pause trigger group '" + matcher + "': " + e.Message, e);
            }
        }

        protected override Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectPausedTriggerGroups(conn, cancellationToken);
        }

        protected override async Task<IReadOnlyCollection<string>> ResumeTriggers(
            ConnectionAndTransactionHolder conn,
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken)
        {
            await Delegate.DeletePausedTriggerGroup(conn, matcher, cancellationToken).ConfigureAwait(false);
            var groups = new ReadOnlyCompatibleHashSet<string>();

            IReadOnlyCollection<TriggerKey> keys = await Delegate.SelectTriggersInGroup(conn, matcher, cancellationToken).ConfigureAwait(false);

            foreach (TriggerKey key in keys)
            {
                await ResumeTrigger(conn, key, cancellationToken).ConfigureAwait(false);
                groups.Add(key.Group);
            }

            return groups;

            // TODO: find an efficient way to resume triggers (better than the
            // above)... logic below is broken because of
            // findTriggersToBeBlocked()
            /*
            * int res =
            * getDelegate().UpdateTriggerGroupStateFromOtherState(conn,
            * groupName, StateWaiting, StatePaused);
            *
            * if(res > 0) {
            *
            * long misfireTime = System.currentTimeMillis();
            * if(getMisfireThreshold() > 0) misfireTime -=
            * getMisfireThreshold();
            *
            * Key[] misfires =
            * getDelegate().SelectMisfiredTriggersInGroupInState(conn,
            * groupName, StateWaiting, misfireTime);
            *
            * List blockedTriggers = findTriggersToBeBlocked(conn,
            * groupName);
            *
            * Iterator itr = blockedTriggers.iterator(); while(itr.hasNext()) {
            * Key key = (Key)itr.next();
            * getDelegate().UpdateTriggerState(conn, key.getName(),
            * key.getGroup(), StateBlocked); }
            *
            * for(int i=0; i < misfires.length; i++) {               String
            * newState = StateWaiting;
            * if(blockedTriggers.contains(misfires[i])) newState =
            * StateBlocked; UpdateMisfiredTrigger(conn,
            * misfires[i].getName(), misfires[i].getGroup(), newState, true); } }
            */
        }

        protected override async Task PauseAll(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            var groupNames = await GetTriggerGroupNames(conn, cancellationToken).ConfigureAwait(false);

            foreach (string groupName in groupNames)
            {
                await PauseTriggerGroup(conn, GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken).ConfigureAwait(false);
            }

            try
            {
                if (!await Delegate.IsTriggerGroupPaused(conn, AdoConstants.AllGroupsPaused, cancellationToken).ConfigureAwait(false))
                {
                    await Delegate.InsertPausedTriggerGroup(conn, AdoConstants.AllGroupsPaused, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't pause all trigger groups: " + e.Message, e);
            }
        }

        protected override Task ClearAllTriggerGroupsPausedFlag(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken)
        {
            return Delegate.DeletePausedTriggerGroup(conn, AdoConstants.AllGroupsPaused, cancellationToken);
        }

        /// <inheritdoc />
        public override async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = default)
        {
            LockType lockName;
            if (AcquireTriggersWithinLock || maxCount > 1)
            {
                lockName = LockType.TriggerAccess;
            }
            else
            {
                lockName = LockType.None;
            }

            return await ExecuteInNonManagedTXLock(
                lockName,
                conn => AcquireNextTrigger(conn, noLaterThan, maxCount, timeWindow, cancellationToken), async (conn, result) =>
                {
                    try
                    {
                        var acquired = await Delegate.SelectInstancesFiredTriggerRecords(conn, InstanceId, cancellationToken).ConfigureAwait(false);
                        var fireInstanceIds = new HashSet<string>();
                        foreach (FiredTriggerRecord ft in acquired)
                        {
                            fireInstanceIds.Add(ft.FireInstanceId);
                        }
                        foreach (IOperableTrigger tr in result)
                        {
                            if (fireInstanceIds.Contains(tr.FireInstanceId))
                            {
                                return true;
                            }
                        }
                        return false;
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("error validating trigger acquisition", e);
                    }
                },
                cancellationToken).ConfigureAwait(false);
        }

        // TODO: this really ought to return something like a FiredTriggerBundle,
        // so that the fireInstanceId doesn't have to be on the trigger...

        protected virtual async Task<IReadOnlyList<IOperableTrigger>> AcquireNextTrigger(
            ConnectionAndTransactionHolder conn,
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = default)
        {
            if (timeWindow < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeWindow));
            }

            List<IOperableTrigger> acquiredTriggers = new List<IOperableTrigger>();
            HashSet<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            const int MaxDoLoopRetry = 3;
            int currentLoopCount = 0;

            do
            {
                currentLoopCount++;
                try
                {
                    var keys = await Delegate.SelectTriggerToAcquire(conn, noLaterThan + timeWindow, MisfireTime, maxCount, cancellationToken).ConfigureAwait(false);

                    // No trigger is ready to fire yet.
                    if (keys == null || keys.Count == 0)
                    {
                        return acquiredTriggers;
                    }

                    DateTimeOffset batchEnd = noLaterThan;

                    foreach (TriggerKey triggerKey in keys)
                    {
                        // If our trigger is no longer available, try a new one.
                        IOperableTrigger nextTrigger = await RetrieveTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false);
                        if (nextTrigger == null)
                        {
                            continue; // next trigger
                        }

                        // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                        // put it back into the timeTriggers set and continue to search for next trigger.
                        JobKey jobKey = nextTrigger.JobKey;
                        IJobDetail job;
                        try
                        {
                            job = await RetrieveJob(conn, jobKey, cancellationToken).ConfigureAwait(false);
                        }
                        catch (JobPersistenceException jpe)
                        {
                            try
                            {
                                Log.ErrorException("Error retrieving job, setting trigger state to ERROR.", jpe);
                                await Delegate.UpdateTriggerState(conn, triggerKey, AdoConstants.StateError, cancellationToken).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                Log.ErrorException("Unable to set trigger state to ERROR.", ex);
                            }
                            continue;
                        }

                        if (job.ConcurrentExecutionDisallowed)
                        {
                            if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                            {
                                continue; // next trigger
                            }
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }

                        if (nextTrigger.GetNextFireTimeUtc() > batchEnd)
                        {
                            break;
                        }

                        // We now have a acquired trigger, let's add to return list.
                        // If our trigger was no longer in the expected state, try a new one.
                        int rowsUpdated = await Delegate.UpdateTriggerStateFromOtherState(
                            conn, 
                            triggerKey, 
                            newState: AdoConstants.StateAcquired, 
                            oldState: AdoConstants.StateWaiting, 
                            cancellationToken).ConfigureAwait(false);
                        
                        if (rowsUpdated <= 0)
                        {
                            // TODO: Hum... shouldn't we log a warning here?
                            continue; // next trigger
                        }
                        nextTrigger.FireInstanceId = GetFiredTriggerRecordId();
                        await Delegate.InsertFiredTrigger(
                            conn, 
                            nextTrigger, 
                            state: AdoConstants.StateAcquired, 
                            jobDetail: null, 
                            cancellationToken).ConfigureAwait(false);

                        if (acquiredTriggers.Count == 0)
                        {
                            var now = SystemTime.UtcNow();
                            var nextFireTime = nextTrigger.GetNextFireTimeUtc().GetValueOrDefault(DateTimeOffset.MinValue);
                            var max = now > nextFireTime ? now : nextFireTime;

                            batchEnd = max + timeWindow;
                        }

                        acquiredTriggers.Add(nextTrigger);
                    }

                    // if we didn't end up with any trigger to fire from that first
                    // batch, try again for another batch. We allow with a max retry count.
                    if (acquiredTriggers.Count == 0 && currentLoopCount < MaxDoLoopRetry)
                    {
                        continue;
                    }

                    // We are done with the while loop.
                    break;
                }
                catch (Exception e)
                {
                    throw new JobPersistenceException("Couldn't acquire next trigger: " + e.Message, e);
                }
            } while (true);

            // Return the acquired trigger list
            return acquiredTriggers;
        }

        public override async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = default)
        {
            return await ExecuteInNonManagedTXLock(
                LockType.TriggerAccess, 
                async conn =>
                {
                    List<TriggerFiredResult> results = new List<TriggerFiredResult>();

                    foreach (IOperableTrigger trigger in triggers)
                    {
                        TriggerFiredResult result;
                        try
                        {
                            TriggerFiredBundle bundle = await TriggerFired(conn, trigger, cancellationToken).ConfigureAwait(false);
                            result = new TriggerFiredResult(bundle);
                        }
                        catch (JobPersistenceException jpe)
                        {
                            Log.ErrorFormat("Caught job persistence exception: " + jpe.Message, jpe);
                            result = new TriggerFiredResult(jpe);
                        }
                        catch (Exception ex)
                        {
                            Log.ErrorFormat("Caught exception: " + ex.Message, ex);
                            result = new TriggerFiredResult(ex);
                        }
                        results.Add(result);
                    }

                    return results;
                },
                async (conn, result) =>
                {
                    try
                    {
                        var acquired = await Delegate.SelectInstancesFiredTriggerRecords(conn, InstanceId, cancellationToken).ConfigureAwait(false);
                        var executingTriggers = new HashSet<string>();
                        foreach (FiredTriggerRecord ft in acquired)
                        {
                            if (ft.FireInstanceState == InternalTriggerState.Executing)
                            {
                                executingTriggers.Add(ft.FireInstanceId);
                            }
                        }
                        foreach (TriggerFiredResult tr in result)
                        {
                            if (tr.TriggerFiredBundle != null && executingTriggers.Contains(tr.TriggerFiredBundle.Trigger.FireInstanceId))
                            {
                                return true;
                            }
                        }
                        return false;
                    }
                    catch (Exception e)
                    {
                        throw new JobPersistenceException("error validating trigger acquisition", e);
                    }
                },
                cancellationToken).ConfigureAwait(false);
        }

        protected virtual async Task<TriggerFiredBundle> TriggerFired(
            ConnectionAndTransactionHolder conn,
            IOperableTrigger trigger,
            CancellationToken cancellationToken = default)
        {
            IJobDetail job;
            ICalendar cal = null;

            // Make sure trigger wasn't deleted, paused, or completed...
            try
            {
                // if trigger was deleted, state will be StateDeleted
                string state = await Delegate.SelectTriggerState(conn, trigger.Key, cancellationToken).ConfigureAwait(false);
                if (!state.Equals(AdoConstants.StateAcquired))
                {
                    return null;
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't select trigger state: " + e.Message, e);
            }

            try
            {
                job = await RetrieveJob(conn, trigger.JobKey, cancellationToken).ConfigureAwait(false);
                if (job == null)
                {
                    return null;
                }
            }
            catch (JobPersistenceException jpe)
            {
                try
                {
                    Log.ErrorException("Error retrieving job, setting trigger state to ERROR.", jpe);
                    await Delegate.UpdateTriggerState(conn, trigger.Key, AdoConstants.StateError, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception sqle)
                {
                    Log.ErrorException("Unable to set trigger state to ERROR.", sqle);
                }
                throw;
            }

            if (trigger.CalendarName != null)
            {
                cal = await RetrieveCalendar(conn, trigger.CalendarName, cancellationToken).ConfigureAwait(false);
                if (cal == null)
                {
                    return null;
                }
            }

            try
            {
                await Delegate.UpdateFiredTrigger(conn, trigger, AdoConstants.StateExecuting, job, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't update fired trigger: " + e.Message, e);
            }

            DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();

            // call triggered - to update the trigger's next-fire-time state...
            trigger.Triggered(cal);

            var state2 = InternalTriggerState.Waiting;
            bool force = true;

            if (job.ConcurrentExecutionDisallowed)
            {
                state2 = InternalTriggerState.Blocked;
                force = false;
                try
                {
                    await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, job.Key, AdoConstants.StateBlocked, AdoConstants.StateWaiting, cancellationToken).ConfigureAwait(false);
                    await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, job.Key, AdoConstants.StateBlocked, AdoConstants.StateAcquired, cancellationToken).ConfigureAwait(false);
                    await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, job.Key, AdoConstants.StatePausedBlocked, AdoConstants.StatePaused, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new JobPersistenceException("Couldn't update states of blocked triggers: " + e.Message, e);
                }
            }

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                state2 = InternalTriggerState.Complete;
                force = true;
            }

            await StoreTrigger(conn, trigger, job, true, state2, force, false, cancellationToken).ConfigureAwait(false);

            job.JobDataMap.ClearDirtyFlag();

            return new TriggerFiredBundle(
                job,
                trigger,
                cal,
                trigger.Key.Group.Equals(SchedulerConstants.DefaultRecoveryGroup),
                SystemTime.UtcNow(),
                trigger.GetPreviousFireTimeUtc(),
                prevFireTime,
                trigger.GetNextFireTimeUtc());
        }

        //---------------------------------------------------------------------------
        // Management methods
        //---------------------------------------------------------------------------

        async Task<RecoverMisfiredJobsResult> IMisfireHandlerOperations.RecoverMisfires(
            Guid requestorId,
            CancellationToken cancellationToken)
        {
            bool transOwner = false;
            ConnectionAndTransactionHolder conn = GetNonManagedTXConnection();
            try
            {
                RecoverMisfiredJobsResult result = RecoverMisfiredJobsResult.NoOp;

                // Before we make the potentially expensive call to acquire the
                // trigger lock, peek ahead to see if it is likely we would find
                // misfired triggers requiring recovery.
                int misfireCount = DoubleCheckLockMisfireHandler
                    ? await Delegate.CountMisfiredTriggersInState(conn, AdoConstants.StateWaiting, MisfireTime, cancellationToken).ConfigureAwait(false)
                    : int.MaxValue;

                if (Log.IsDebugEnabled())
                {
                    Log.DebugFormat("Found {0} triggers that missed their scheduled fire-time.", misfireCount);
                }

                if (misfireCount > 0)
                {
                    transOwner = await LockHandler.ObtainLock(requestorId, conn, LockTriggerAccess, cancellationToken).ConfigureAwait(false);

                    result = await RecoverMisfiredJobs(conn, false, cancellationToken).ConfigureAwait(false);
                }

                CommitConnection(conn, false);
                return result;
            }
            catch (JobPersistenceException jpe)
            {
                RollbackConnection(conn, jpe);
                throw;
            }
            catch (Exception e)
            {
                RollbackConnection(conn, e);
                throw new JobPersistenceException("Database error recovering from misfires.", e);
            }
            finally
            {
                if (transOwner)
                {
                    try
                    {
                        await ReleaseLock(requestorId, LockType.TriggerAccess, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        CleanupConnection(conn);
                    }
                }
            }
        }

        //---------------------------------------------------------------------------
        // Cluster management methods
        //---------------------------------------------------------------------------

        protected bool firstCheckIn = true;

        public DateTimeOffset LastCheckin { get; protected set; } = SystemTime.UtcNow();

        async Task<bool> IClusterManagementOperations.CheckCluster(Guid requestorId, CancellationToken cancellationToken)
        {
            bool transOwner = false;
            bool transStateOwner = false;
            bool recovered = false;

            ConnectionAndTransactionHolder conn = GetNonManagedTXConnection();
            try
            {
                // Other than the first time, always checkin first to make sure there is
                // work to be done before we acquire the lock (since that is expensive,
                // and is almost never necessary).  This must be done in a separate
                // transaction to prevent a deadlock under recovery conditions.
                IReadOnlyList<SchedulerStateRecord> failedRecords = null;
                if (!firstCheckIn)
                {
                    failedRecords = await ClusterCheckIn(conn, cancellationToken).ConfigureAwait(false);
                    CommitConnection(conn, true);
                }

                if (firstCheckIn || failedRecords != null && failedRecords.Count > 0)
                {
                    await LockHandler.ObtainLock(requestorId, conn, LockStateAccess, cancellationToken).ConfigureAwait(false);
                    transStateOwner = true;

                    // Now that we own the lock, make sure we still have work to do.
                    // The first time through, we also need to make sure we update/create our state record
                    if (firstCheckIn)
                    {
                        failedRecords = await ClusterCheckIn(conn, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        failedRecords = await FindFailedInstances(conn, cancellationToken).ConfigureAwait(false);
                    }

                    if (failedRecords.Count > 0)
                    {
                        await LockHandler.ObtainLock(requestorId, conn, LockTriggerAccess, cancellationToken).ConfigureAwait(false);
                        //getLockHandler().obtainLock(conn, LockJobAccess);
                        transOwner = true;

                        await ClusterRecover(conn, failedRecords, cancellationToken).ConfigureAwait(false);
                        recovered = true;
                    }
                }

                CommitConnection(conn, false);
            }
            catch (JobPersistenceException jpe)
            {
                RollbackConnection(conn, jpe);
                throw;
            }
            finally
            {
                try
                {
                    if (transOwner)
                    {
                        await ReleaseLock(requestorId, LockType.TriggerAccess, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (transStateOwner)
                    {
                        try
                        {
                            await ReleaseLock(requestorId, LockType.StateAccess, cancellationToken).ConfigureAwait(false);
                        }
                        finally
                        {
                            CleanupConnection(conn);
                        }
                    }
                }
            }

            firstCheckIn = false;

            return recovered;
        }

        /// <summary>
        /// Get a list of all scheduler instances in the cluster that may have failed.
        /// This includes this scheduler if it is checking in for the first time.
        /// </summary>
        protected virtual async Task<IReadOnlyList<SchedulerStateRecord>> FindFailedInstances(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            try
            {
                List<SchedulerStateRecord> failedInstances = new List<SchedulerStateRecord>();
                bool foundThisScheduler = false;

                var states = await Delegate.SelectSchedulerStateRecords(conn, null, cancellationToken).ConfigureAwait(false);

                foreach (SchedulerStateRecord rec in states)
                {
                    // find own record...
                    if (rec.SchedulerInstanceId.Equals(InstanceId))
                    {
                        foundThisScheduler = true;
                        if (firstCheckIn)
                        {
                            failedInstances.Add(rec);
                        }
                    }
                    else
                    {
                        // find failed instances...
                        if (CalcFailedIfAfter(rec) < SystemTime.UtcNow())
                        {
                            failedInstances.Add(rec);
                        }
                    }
                }

                // The first time through, also check for orphaned fired triggers.
                if (firstCheckIn)
                {
                    failedInstances.AddRange(await FindOrphanedFailedInstances(conn, states, cancellationToken).ConfigureAwait(false));
                }

                // If not the first time but we didn't find our own instance, then
                // Someone must have done recovery for us.
                if (!foundThisScheduler && !firstCheckIn)
                {
                    // TODO: revisit when handle self-failed-out impl'ed (see TODO in clusterCheckIn() below)
                    Log.Warn(
                        "This scheduler instance (" + InstanceId + ") is still " +
                        "active but was recovered by another instance in the cluster.  " +
                        "This may cause inconsistent behavior.");
                }

                return failedInstances;
            }
            catch (Exception e)
            {
                LastCheckin = SystemTime.UtcNow();
                throw new JobPersistenceException("Failure identifying failed instances when checking-in: "
                                                  + e.Message, e);
            }
        }

        /// <summary>
        /// Create dummy <see cref="SchedulerStateRecord" /> objects for fired triggers
        /// that have no scheduler state record.  Checkin timestamp and interval are
        /// left as zero on these dummy <see cref="SchedulerStateRecord" /> objects.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="schedulerStateRecords">List of all current <see cref="SchedulerStateRecord" />s</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        private async Task<IReadOnlyList<SchedulerStateRecord>> FindOrphanedFailedInstances(
            ConnectionAndTransactionHolder conn,
            IReadOnlyCollection<SchedulerStateRecord> schedulerStateRecords,
            CancellationToken cancellationToken)
        {
            List<SchedulerStateRecord> orphanedInstances = new List<SchedulerStateRecord>();

            var names = await Delegate.SelectFiredTriggerInstanceNames(conn, cancellationToken).ConfigureAwait(false);
            var allFiredTriggerInstanceNames = new HashSet<string>(names);
            if (allFiredTriggerInstanceNames.Count > 0)
            {
                foreach (SchedulerStateRecord rec in schedulerStateRecords)
                {
                    allFiredTriggerInstanceNames.Remove(rec.SchedulerInstanceId);
                }

                foreach (string name in allFiredTriggerInstanceNames)
                {
                    SchedulerStateRecord orphanedInstance = new SchedulerStateRecord();
                    orphanedInstance.SchedulerInstanceId = name;

                    orphanedInstances.Add(orphanedInstance);

                    Log.Warn("Found orphaned fired triggers for instance: " + orphanedInstance.SchedulerInstanceId);
                }
            }

            return orphanedInstances;
        }

        protected DateTimeOffset CalcFailedIfAfter(SchedulerStateRecord rec)
        {
            TimeSpan passed = SystemTime.UtcNow() - LastCheckin;
            TimeSpan ts = rec.CheckinInterval > passed ? rec.CheckinInterval : passed;
            return rec.CheckinTimestamp.Add(ts).Add(TimeSpan.FromMilliseconds(7500));
        }

        protected virtual async Task<IReadOnlyList<SchedulerStateRecord>> ClusterCheckIn(
            ConnectionAndTransactionHolder conn,
            CancellationToken cancellationToken = default)
        {
            var failedInstances = await FindFailedInstances(conn, cancellationToken).ConfigureAwait(false);
            try
            {
                // TODO: handle self-failed-out

                // check in...
                LastCheckin = SystemTime.UtcNow();
                if (await Delegate.UpdateSchedulerState(conn, InstanceId, LastCheckin, cancellationToken).ConfigureAwait(false) == 0)
                {
                    await Delegate.InsertSchedulerState(conn, InstanceId, LastCheckin, ClusterCheckinInterval, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Failure updating scheduler state when checking-in: " + e.Message, e);
            }

            return failedInstances;
        }

        protected virtual async Task ClusterRecover(
            ConnectionAndTransactionHolder conn,
            IReadOnlyList<SchedulerStateRecord> failedInstances,
            CancellationToken cancellationToken = default)
        {
            if (failedInstances.Count > 0)
            {
                long recoverIds = SystemTime.UtcNow().Ticks;

                LogWarnIfNonZero(failedInstances.Count,
                    "ClusterManager: detected " + failedInstances.Count + " failed or restarted instances.");
                try
                {
                    foreach (SchedulerStateRecord rec in failedInstances)
                    {
                        Log.Info("ClusterManager: Scanning for instance \"" + rec.SchedulerInstanceId +
                                 "\"'s failed in-progress jobs.");

                        var firedTriggerRecs = await Delegate.SelectInstancesFiredTriggerRecords(conn, rec.SchedulerInstanceId, cancellationToken).ConfigureAwait(false);

                        int acquiredCount = 0;
                        int recoveredCount = 0;
                        int otherCount = 0;

                        var triggerKeys = new HashSet<TriggerKey>();

                        foreach (FiredTriggerRecord ftRec in firedTriggerRecs)
                        {
                            TriggerKey tKey = ftRec.TriggerKey;
                            JobKey jKey = ftRec.JobKey;

                            triggerKeys.Add(tKey);

                            // release blocked triggers..
                            if (ftRec.FireInstanceState == InternalTriggerState.Blocked)
                            {
                                await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, jKey, AdoConstants.StateWaiting, AdoConstants.StateBlocked, cancellationToken).ConfigureAwait(false);
                            }
                            else if (ftRec.FireInstanceState == InternalTriggerState.PausedAndBlocked)
                            {
                                await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, jKey, AdoConstants.StatePaused, AdoConstants.StatePausedBlocked, cancellationToken).ConfigureAwait(false);
                            }

                            // release acquired triggers..
                            if (ftRec.FireInstanceState == InternalTriggerState.Acquired)
                            {
                                await Delegate.UpdateTriggerStateFromOtherState(conn, tKey, AdoConstants.StateWaiting, AdoConstants.StateAcquired, cancellationToken).ConfigureAwait(false);
                                acquiredCount++;
                            }
                            else if (ftRec.JobRequestsRecovery)
                            {
                                // handle jobs marked for recovery that were not fully
                                // executed..
                                if (await JobExists(conn, jKey, cancellationToken).ConfigureAwait(false))
                                {
                                    SimpleTriggerImpl rcvryTrig =
                                        new SimpleTriggerImpl(
                                            "recover_" + rec.SchedulerInstanceId + "_" + Convert.ToString(recoverIds++, CultureInfo.InvariantCulture),
                                            SchedulerConstants.DefaultRecoveryGroup, ftRec.FireTimestamp);

                                    rcvryTrig.JobName = jKey.Name;
                                    rcvryTrig.JobGroup = jKey.Group;
                                    rcvryTrig.MisfireInstruction = MisfireInstruction.SimpleTrigger.FireNow;
                                    rcvryTrig.Priority = ftRec.Priority;
                                    JobDataMap jd = await Delegate.SelectTriggerJobDataMap(conn, tKey, cancellationToken).ConfigureAwait(false);
                                    jd.Put(SchedulerConstants.FailedJobOriginalTriggerName, tKey.Name);
                                    jd.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, tKey.Group);
                                    jd.Put(SchedulerConstants.FailedJobOriginalTriggerFiretime, Convert.ToString(ftRec.FireTimestamp, CultureInfo.InvariantCulture));
                                    rcvryTrig.JobDataMap = jd;

                                    rcvryTrig.ComputeFirstFireTimeUtc(null);
                                    await StoreTrigger(conn, rcvryTrig, null, false, InternalTriggerState.Waiting, false, true, cancellationToken).ConfigureAwait(false);
                                    recoveredCount++;
                                }
                                else
                                {
                                    Log.Warn("ClusterManager: failed job '" + jKey +
                                             "' no longer exists, cannot schedule recovery.");
                                    otherCount++;
                                }
                            }
                            else
                            {
                                otherCount++;
                            }

                            // free up stateful job's triggers
                            if (ftRec.JobDisallowsConcurrentExecution)
                            {
                                await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, jKey, AdoConstants.StateWaiting, AdoConstants.StateBlocked, cancellationToken).ConfigureAwait(false);
                                await Delegate.UpdateTriggerStatesForJobFromOtherState(conn, jKey, AdoConstants.StatePaused, AdoConstants.StatePausedBlocked, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        await Delegate.DeleteFiredTriggers(conn, rec.SchedulerInstanceId, cancellationToken).ConfigureAwait(false);

                        // Check if any of the fired triggers we just deleted were the last fired trigger
                        // records of a COMPLETE trigger.
                        int completeCount = 0;
                        foreach (TriggerKey triggerKey in triggerKeys)
                        {
                            if (AdoConstants.StateComplete.Equals(await Delegate.SelectTriggerState(conn, triggerKey, cancellationToken).ConfigureAwait(false)))
                            {
                                var firedTriggers = await Delegate.SelectFiredTriggerRecords(conn, triggerKey.Name, triggerKey.Group, cancellationToken).ConfigureAwait(false);
                                if (firedTriggers.Count == 0)
                                {
                                    if (await RemoveTrigger(conn, triggerKey, cancellationToken).ConfigureAwait(false))
                                    {
                                        completeCount++;
                                    }
                                }
                            }
                        }
                        LogWarnIfNonZero(acquiredCount,
                            "ClusterManager: ......Freed " + acquiredCount + " acquired trigger(s).");
                        LogWarnIfNonZero(completeCount,
                            "ClusterManager: ......Deleted " + completeCount + " complete triggers(s).");
                        LogWarnIfNonZero(recoveredCount,
                            "ClusterManager: ......Scheduled " + recoveredCount +
                            " recoverable job(s) for recovery.");
                        LogWarnIfNonZero(otherCount,
                            "ClusterManager: ......Cleaned-up " + otherCount + " other failed job(s).");

                        if (rec.SchedulerInstanceId.Equals(InstanceId) == false)
                        {
                            await Delegate.DeleteSchedulerState(conn, rec.SchedulerInstanceId, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new JobPersistenceException("Failure recovering jobs: " + e.Message, e);
                }
            }
        }

        protected virtual void LogWarnIfNonZero(int val, string warning)
        {
            if (val > 0)
            {
                Log.Info(warning);
            }
            else
            {
                Log.Debug(warning);
            }
        }

        /// <summary>
        /// Cleanup the given database connection.  This means restoring
        /// any modified auto commit or transaction isolation connection
        /// attributes, and then closing the underlying connection.
        /// </summary>
        ///
        /// <remarks>
        /// This is separate from closeConnection() because the Spring
        /// integration relies on being able to overload closeConnection() and
        /// expects the same connection back that it originally returned
        /// from the datasource.
        /// </remarks>
        /// <seealso cref="CloseConnection(ConnectionAndTransactionHolder)" />
        protected virtual void CleanupConnection(ConnectionAndTransactionHolder conn)
        {
            if (conn != null)
            {
                CloseConnection(conn);
            }
        }

        /// <summary>
        /// Closes the supplied connection.
        /// </summary>
        /// <param name="cth">(Optional)</param>
        protected virtual void CloseConnection(ConnectionAndTransactionHolder cth)
        {
            cth.Close();
        }

        /// <summary>
        /// Rollback the supplied connection.
        /// </summary>
        protected virtual void RollbackConnection(ConnectionAndTransactionHolder cth, Exception cause)
        {
            if (cth == null)
            {
                // db might be down or similar
                Log.Info("ConnectionAndTransactionHolder passed to RollbackConnection was null, ignoring");
                return;
            }

            cth.Rollback(IsTransient(cause));
        }

        protected virtual bool IsTransient(Exception ex)
        {
            SqlException sqlException = ex as SqlException ?? ex?.InnerException as SqlException;
            if (sqlException != null)
            {
                switch (sqlException.Number)
                {
                    // SQL Error Code: 11001
                    // A network-related or instance-specific error occurred while establishing a connection to SQL Server.
                    // The server was not found or was not accessible. Verify that the instance name is correct and that SQL
                    // Server is configured to allow remote connections. (provider: TCP Provider, error: 0 - No such host is known.)
                    case 11001:
                        return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Commit the supplied connection.
        /// </summary>
        /// <param name="cth">The CTH.</param>
        /// <param name="openNewTransaction">if set to <c>true</c> opens a new transaction.</param>
        /// <throws>JobPersistenceException thrown if a SQLException occurs when the </throws>
        protected virtual void CommitConnection(ConnectionAndTransactionHolder cth, bool openNewTransaction)
        {
            if (cth == null)
            {
                Log.Debug("ConnectionAndTransactionHolder passed to CommitConnection was null, ignoring");
                return;
            }
            cth.Commit(openNewTransaction);
        }

        protected override Task UpdateTriggerStatesForJobFromOtherState(
            ConnectionAndTransactionHolder conn,
            JobKey jobKey,
            InternalTriggerState newState,
            InternalTriggerState oldState, 
            CancellationToken cancellationToken)
        {
            return Delegate.UpdateTriggerStatesForJobFromOtherState(
                conn,
                jobKey,
                GetStateName(newState),
                GetStateName(oldState),
                cancellationToken);
        }

        protected override Task UpdateJobData(
            ConnectionAndTransactionHolder conn, 
            IJobDetail jobDetail, 
            CancellationToken cancellationToken)
        {
            return Delegate.UpdateJobData(conn, jobDetail, cancellationToken);
        }

        protected override Task DeleteFiredTrigger(
            ConnectionAndTransactionHolder conn,
            string triggerFireInstanceId, 
            CancellationToken cancellationToken)
        {
            return Delegate.DeleteFiredTrigger(conn, triggerFireInstanceId, cancellationToken);
        }

        protected override Task UpdateTriggerStatesForJob(
            ConnectionAndTransactionHolder conn, 
            JobKey triggerJobKey,
            InternalTriggerState state, 
            CancellationToken cancellationToken)
        {
            return Delegate.UpdateTriggerStatesForJob(conn, triggerJobKey, GetStateName(state), cancellationToken);
        }

        protected override Task UpdateTriggerState(
            ConnectionAndTransactionHolder conn, 
            TriggerKey triggerKey, 
            InternalTriggerState state,
            CancellationToken cancellationToken)
        {
            return Delegate.UpdateTriggerState(conn, triggerKey, GetStateName(state), cancellationToken);
        }

        protected override async Task RetryExecuteInNonManagedTXLock(
            LockType lockType,
            Func<ConnectionAndTransactionHolder, Task> txCallback,
            CancellationToken cancellationToken)
        {
            await RetryExecuteInNonManagedTXLock<object>(lockType, async holder =>
            {
                await txCallback(holder).ConfigureAwait(false);
                return null;
            }, cancellationToken).ConfigureAwait(false);
        }

        protected virtual async Task<T> RetryExecuteInNonManagedTXLock<T>(
            LockType lockType,
            Func<ConnectionAndTransactionHolder, Task<T>> txCallback,
            CancellationToken cancellationToken = default)
        {
            for (int retry = 1; !IsShutdown; retry++)
            {
                try
                {
                    return await ExecuteInNonManagedTXLock(lockType, txCallback, null, cancellationToken).ConfigureAwait(false);
                }
                catch (JobPersistenceException jpe)
                {
                    if (retry%RetryableActionErrorLogThreshold == 0)
                    {
                        await SchedulerSignaler.NotifySchedulerListenersError("An error occurred while " + txCallback, jpe, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    Log.ErrorException("retryExecuteInNonManagedTXLock: RuntimeException " + e.Message, e);
                }

                // retry every N seconds (the db connection must be failed)
                await Task.Delay(DbRetryInterval, cancellationToken).ConfigureAwait(false);
            }

            throw new InvalidOperationException("JobStore is shutdown - aborting retry");
        }

        protected async Task ExecuteInNonManagedTXLock(
            LockType lockType,
            Func<ConnectionAndTransactionHolder, Task> txCallback,
            CancellationToken cancellationToken)
        {
            await ExecuteInNonManagedTXLock(lockType, async conn =>
            {
                await txCallback(conn).ConfigureAwait(false);
                return true;
            }, cancellationToken).ConfigureAwait(false);
        }

        protected Task<T> ExecuteInNonManagedTXLock<T>(
            LockType lockType,
            Func<ConnectionAndTransactionHolder, Task<T>> txCallback,
            CancellationToken cancellationToken)
        {
            return ExecuteInNonManagedTXLock(lockType, txCallback, null, cancellationToken);
        }

        /// <summary>
        /// Execute the given callback having optionally acquired the given lock.
        /// This uses the non-managed transaction connection.
        /// </summary>
        /// <param name="lockType">
        /// The name of the lock to acquire, for example
        /// "TRIGGER_ACCESS".  If null, then no lock is acquired, but the
        /// lockCallback is still executed in a non-managed transaction.
        /// </param>
        /// <param name="txCallback">
        /// The callback to execute after having acquired the given lock.
        /// </param>
        /// <param name="txValidator"></param>>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        protected async Task<T> ExecuteInNonManagedTXLock<T>(
            LockType lockType,
            Func<ConnectionAndTransactionHolder, Task<T>> txCallback,
            Func<ConnectionAndTransactionHolder, T, Task<bool>> txValidator,
            CancellationToken cancellationToken)
        {
            bool transOwner = false;
            Guid requestorId = Guid.NewGuid();
            ConnectionAndTransactionHolder conn = null;
            try
            {
                if (lockType != LockType.None)
                {
                    // If we aren't using db locks, then delay getting DB connection
                    // until after acquiring the lock since it isn't needed.
                    if (LockHandler.RequiresConnection)
                    {
                        conn = GetNonManagedTXConnection();
                    }

                    transOwner = await LockHandler.ObtainLock(requestorId, conn, GetLockName(lockType), cancellationToken).ConfigureAwait(false);
                }

                if (conn == null)
                {
                    conn = GetNonManagedTXConnection();
                }

                T result = await txCallback(conn).ConfigureAwait(false);
                try
                {
                    CommitConnection(conn, false);
                }
                catch (JobPersistenceException jpe)
                {
                    RollbackConnection(conn, jpe);
                    if (txValidator == null)
                    {
                        throw;
                    }
                    if (!await RetryExecuteInNonManagedTXLock(
                        lockType,
                        async connection => await txValidator(connection, result).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false))
                    {
                        throw;
                    }
                }

                DateTimeOffset? sigTime = conn.SignalSchedulingChangeOnTxCompletion;
                if (sigTime != null)
                {
                    await SchedulerSignaler.SignalSchedulingChange(sigTime, CancellationToken.None).ConfigureAwait(false);
                }

                return result;
            }
            catch (JobPersistenceException jpe)
            {
                RollbackConnection(conn, jpe);
                throw;
            }
            catch (Exception e)
            {
                RollbackConnection(conn, e);
                throw new JobPersistenceException("Unexpected runtime exception: " + e.Message, e);
            }
            finally
            {
                if (transOwner)
                {
                    try
                    {
                        await ReleaseLock(requestorId, lockType, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        CleanupConnection(conn);
                    }
                }
            }
        }

        protected override IClusterManagementOperations ClusterManagementOperations => this;

        protected override IMisfireHandlerOperations MisfireHandlerOperations => this;

        protected override Task<TriggerStatus> GetTriggerStatus(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            CancellationToken cancellationToken)
        {
            return Delegate.SelectTriggerStatus(conn, triggerKey, cancellationToken);
        }

        protected override Task<int> UpdateTriggerStateFromOtherStates(
            ConnectionAndTransactionHolder conn,
            TriggerKey triggerKey,
            InternalTriggerState newState,
            InternalTriggerState[] oldStates,
            CancellationToken cancellationToken)
        {
            string newStateString = GetStateName(newState);
            string oldState1 = GetStateName(oldStates[0]);
            string oldState2 = oldStates.Length > 1 ? GetStateName(oldStates[1]) : oldState1;
            string oldState3 = oldStates.Length > 2 ? GetStateName(oldStates[2]) : oldState2;

            return Delegate.UpdateTriggerStateFromOtherStates(
                conn, 
                triggerKey,
                newStateString, 
                oldState1, 
                oldState2, 
                oldState3, 
                cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected string GetLockName(LockType lockType)
        {
            if (lockType == LockType.None)
            {
                return null;
            }

            if (lockType == LockType.TriggerAccess)
            {
                return LockTriggerAccess;
            }

            if (lockType == LockType.StateAccess)
            {
                return LockStateAccess;
            }

            ThrowArgumentOutOfRangeException(nameof(lockType), lockType);
            return null;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetStateName(InternalTriggerState state)
        {
            switch (state)
            {
                case InternalTriggerState.Waiting:
                    return AdoConstants.StateWaiting;
                case InternalTriggerState.Acquired:
                    return AdoConstants.StateAcquired;
                case InternalTriggerState.Executing:
                    return AdoConstants.StateExecuting;
                case InternalTriggerState.Complete:
                    return AdoConstants.StateComplete;
                case InternalTriggerState.Paused:
                    return AdoConstants.StatePaused;
                case InternalTriggerState.Blocked:
                    return AdoConstants.StateBlocked;
                case InternalTriggerState.PausedAndBlocked:
                    return AdoConstants.StatePausedBlocked;
                case InternalTriggerState.Error:
                    return AdoConstants.StateError;
            }

            ThrowArgumentOutOfRangeException(nameof(state), state);
            return null;
        }

        private static void ThrowArgumentOutOfRangeException<T>(string paramName, T value)
        {
            throw new ArgumentOutOfRangeException(paramName, value, null);
        }
    }
}