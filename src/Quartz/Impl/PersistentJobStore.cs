using System;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Impl.AdoJobStore;
using Quartz.Logging;
using Quartz.Spi;

namespace Quartz.Impl
{
    /// <summary>
    /// Base class that tries to be persistence technology agnostic and offers some common boiler plate
    /// code needed to hook job store properly.
    /// </summary>
    public abstract class PersistentJobStore
    {
        private static long firedTriggerCounter = SystemTime.UtcNow().Ticks;

        private ClusterManager clusterManager;
        private MisfireHandler misfireHandler;

        private TimeSpan misfireThreshold = TimeSpan.FromMinutes(1); // one minute
        private TimeSpan? misfireHandlerFrequency;
        private string instanceName;

        protected PersistentJobStore()
        {
            RetryableActionErrorLogThreshold = 4;
            DoubleCheckLockMisfireHandler = true;
            ClusterCheckinInterval = TimeSpan.FromMilliseconds(7500);
            MaxMisfiresToHandleAtATime = 20;
            RetryInterval = TimeSpan.FromSeconds(15);
            Log = LogProvider.GetLogger(GetType());
        }
        
        /// <summary>
        /// Gets the log.
        /// </summary>
        /// <value>The log.</value>
        internal ILog Log { get; }

        protected ITypeLoadHelper TypeLoadHelper { get; set; }
        protected ISchedulerSignaler SchedulerSignaler { get; set; }

        protected bool SchedulerRunning { get; private set; }
        protected bool IsShutdown { get; private set; }

        /// <summary>
        /// Gets or sets the number of retries before an error is logged for recovery operations.
        /// </summary>
        public int RetryableActionErrorLogThreshold { get; set; }

        /// <summary>
        /// Get or set the instance id of the scheduler (must be unique within a cluster).
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Get or set the instance id of the scheduler (must be unique within this server instance).
        /// </summary>
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
        
        public bool SupportsPersistence => true;
        
        /// <summary>
        /// Get or set whether this instance is part of a cluster.
        /// </summary>
        public bool Clustered { get; set; }

        /// <summary>
        /// Get or set the frequency at which this instance "checks-in"
        /// with the other instances of the cluster. -- Affects the rate of
        /// detecting failed instances.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan ClusterCheckinInterval { get; set; }
        
        /// <summary>
        /// Gets or sets the retry interval.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan RetryInterval { get; set; }

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
        public bool DoubleCheckLockMisfireHandler { get; set; }
        
        /// <summary>
        /// Get or set the maximum number of misfired triggers that the misfire handling
        /// thread will try to recover at one time (within one transaction).  The
        /// default is 20.
        /// </summary>
        public int MaxMisfiresToHandleAtATime { get; set; }
        
        public virtual Task Initialize(
            ITypeLoadHelper typeLoadHelper,
            ISchedulerSignaler schedulerSignaler,
            CancellationToken cancellationToken = default)
        {
            TypeLoadHelper = typeLoadHelper;
            SchedulerSignaler = schedulerSignaler;

            return TaskUtil.CompletedTask;
        }

        /// <see cref="IJobStore.SchedulerStarted"/>
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
        
        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has been paused.
        /// </summary>
        public virtual Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            SchedulerRunning = false;
            return TaskUtil.CompletedTask;
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has resumed after being paused.
        /// </summary>
        public virtual Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            SchedulerRunning = true;
            return TaskUtil.CompletedTask;
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// it should free up all of it's resources because the scheduler is
        /// shutting down.
        /// </summary>
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

        protected abstract IClusterManagementOperations ClusterManagementOperations { get; }
        protected abstract IMisfireHandlerOperations MisfireHandlerOperations { get; }
    }
}