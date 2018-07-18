using System;
using System.Threading;
using System.Threading.Tasks;

using Quartz.Logging;
using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    internal sealed class MisfireHandler
    {
        private static readonly ILog log = LogProvider.GetLogger(typeof (MisfireHandler));
        // keep constant lock requestor id for handler's lifetime
        private readonly Guid requestorId = Guid.NewGuid();

        private readonly RavenJobStore jobStore;
        private int numFails;

        private readonly CancellationTokenSource cancellationTokenSource;
        private readonly QueuedTaskScheduler taskScheduler;
        private Task task;

        internal MisfireHandler(RavenJobStore jobStore)
        {
            this.jobStore = jobStore;

            string threadName = $"QuartzScheduler_{jobStore.InstanceName}-{jobStore.InstanceId}_MisfireHandler";
            taskScheduler = new QueuedTaskScheduler(threadCount: 1, threadName: threadName, useForegroundThreads: !jobStore.MakeThreadsDaemons);
            cancellationTokenSource = new CancellationTokenSource();
        }

        public void Initialize()
        {
            task = Task.Factory.StartNew(Run, CancellationToken.None, TaskCreationOptions.HideScheduler, taskScheduler).Unwrap();
        }

        private async Task Run()
        {
            var token = cancellationTokenSource.Token;
            while (!token.IsCancellationRequested)
            {
                token.ThrowIfCancellationRequested();

                DateTimeOffset sTime = SystemTime.UtcNow();

                var recoverMisfiredJobsResult = await Manage().ConfigureAwait(false);

                if (recoverMisfiredJobsResult.ProcessedMisfiredTriggerCount > 0)
                {
                    jobStore.SignalSchedulingChangeImmediately(recoverMisfiredJobsResult.EarliestNewTime);
                }

                token.ThrowIfCancellationRequested();

                TimeSpan timeToSleep = TimeSpan.FromMilliseconds(50); // At least a short pause to help balance threads
                if (!recoverMisfiredJobsResult.HasMoreMisfiredTriggers)
                {
                    timeToSleep = jobStore.MisfireHandlerFrequency - (SystemTime.UtcNow() - sTime);
                    if (timeToSleep <= TimeSpan.Zero)
                    {
                        timeToSleep = TimeSpan.FromMilliseconds(50);
                    }

                    if (numFails > 0)
                    {
                        timeToSleep = jobStore.DbRetryInterval > timeToSleep ? jobStore.DbRetryInterval : timeToSleep;
                    }
                }

                await Task.Delay(timeToSleep, token).ConfigureAwait(false);
            }
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

        private async Task<(bool HasMoreMisfiredTriggers, int ProcessedMisfiredTriggerCount, DateTimeOffset EarliestNewTime)> Manage()
        {
            try
            {
                log.Debug("Scanning for misfires...");

                var res = await jobStore.DoRecoverMisfires(requestorId, CancellationToken.None).ConfigureAwait(false);
                numFails = 0;
                return res;
            }
            catch (Exception e)
            {
                if (numFails%jobStore.RetryableActionErrorLogThreshold == 0)
                {
                    log.ErrorException("Error handling misfires: " + e.Message, e);
                }
                numFails++;
            }
            return (false, 0, DateTimeOffset.MaxValue);
        }
    }
}