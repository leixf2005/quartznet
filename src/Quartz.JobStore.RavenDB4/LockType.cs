namespace Quartz.Impl.RavenDB
{
    internal enum LockType
    {
        None,
        TriggerAccess,
        StateAccess
    }
}