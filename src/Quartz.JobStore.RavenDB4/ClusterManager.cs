using System;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Logging;
using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    internal class ClusterManager
    {
        private static readonly ILog log = LogProvider.GetLogger(typeof(ClusterManager));

        // keep constant lock requestor id for manager's lifetime
        private readonly Guid requestorId = Guid.NewGuid();

        private readonly RavenJobStore jobStore;

        private QueuedTaskScheduler taskScheduler;
        private readonly CancellationTokenSource cancellationTokenSource;
        private Task task;

        private int numFails;

        internal ClusterManager(RavenJobStore jobStore)
        {
            this.jobStore = jobStore;
            cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task Initialize()
        {
            await Manage().ConfigureAwait(false);
            string threadName = $"QuartzScheduler_{jobStore.InstanceName}-{jobStore.InstanceId}_ClusterManager";

            taskScheduler = new QueuedTaskScheduler(threadCount: 1, threadPriority: ThreadPriority.AboveNormal, threadName: threadName, useForegroundThreads: !jobStore.MakeThreadsDaemons);
            task = Task.Factory.StartNew(() => Run(cancellationTokenSource.Token), cancellationTokenSource.Token, TaskCreationOptions.HideScheduler, taskScheduler).Unwrap();
        }

        public async Task Shutdown()
        {
            cancellationTokenSource.Cancel();
            try
            {
                taskScheduler.Dispose();
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task<bool> Manage()
        {
            bool res = false;
            try
            {
                res = await jobStore.DoCheckin(requestorId).ConfigureAwait(false);

                numFails = 0;
                log.Debug("Check-in complete.");
            }
            catch (Exception e)
            {
                if (numFails % jobStore.RetryableActionErrorLogThreshold == 0)
                {
                    log.ErrorException("Error managing cluster: " + e.Message, e);
                }
                numFails++;
            }
            return res;
        }

        private async Task Run(CancellationToken token)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();

                TimeSpan timeToSleep = jobStore.ClusterCheckinInterval;
                TimeSpan transpiredTime = SystemTime.UtcNow() - jobStore.LastCheckin;
                timeToSleep = timeToSleep - transpiredTime;
                if (timeToSleep <= TimeSpan.Zero)
                {
                    timeToSleep = TimeSpan.FromMilliseconds(100);
                }

                if (numFails > 0)
                {
                    timeToSleep = jobStore.DbRetryInterval > timeToSleep ? jobStore.DbRetryInterval : timeToSleep;
                }

                await Task.Delay(timeToSleep, token).ConfigureAwait(false);

                token.ThrowIfCancellationRequested();

                if (await Manage().ConfigureAwait(false))
                {
                    jobStore.SignalSchedulingChangeImmediately(SchedulerConstants.SchedulingSignalDateTime);
                }
            }
            // ReSharper disable once FunctionNeverReturns
        }
    }
}