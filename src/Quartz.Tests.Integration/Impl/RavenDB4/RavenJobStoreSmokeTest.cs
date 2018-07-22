using System.Collections.Specialized;
using System.Reflection;
using System.Threading.Tasks;

using NUnit.Framework;

using Quartz.Impl;
using Quartz.Impl.RavenDB;
using Quartz.Util;

namespace Quartz.Tests.Integration.Impl.RavenDB4
{
    [TestFixture]
    [Category("database")]
    public class RavenJobStoreSmokeTest
    {
        private ILogProvider oldProvider;

        [OneTimeSetUp]
        public void FixtureSetUp()
        {
            // set Adapter to report problems
            oldProvider = (ILogProvider) typeof(LogProvider).GetField("s_currentLogProvider", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null);
            LogProvider.SetCurrentLogProvider(new FailFastLoggerFactoryAdapter());
        }

        [OneTimeTearDown]
        public void FixtureTearDown()
        {
            // default back to old
            LogProvider.SetCurrentLogProvider(oldProvider);
        }

        [Test]
        [Category("ravendb")]
        public async Task TestRavenJobStore()
        {
            NameValueCollection properties = new NameValueCollection();
            properties["quartz.scheduler.instanceName"] = "TestScheduler";
            properties["quartz.scheduler.instanceId"] = "instance_one";
            properties["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz";
            properties["quartz.threadPool.threadCount"] = "10";
            properties["quartz.jobStore.misfireThreshold"] = "60000";
            properties["quartz.jobStore.type"] = typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion();
            properties["quartz.jobStore.url"] = "http://localhost:9999";
            properties["quartz.jobStore.database"] = "quartznet";
            // not really used
            properties["quartz.serializer.type"] = "json";

            // Clear any old errors from the log
            FailFastLoggerFactoryAdapter.Errors.Clear();

            // First we must get a reference to a scheduler
            ISchedulerFactory sf = new StdSchedulerFactory(properties);
            IScheduler sched = await sf.GetScheduler();
            SmokeTestPerformer performer = new SmokeTestPerformer();
            await performer.Test(sched, clearJobs: true, scheduleJobs: true);

            Assert.IsEmpty(FailFastLoggerFactoryAdapter.Errors, "Found error from logging output");
        }
    }
}