using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    internal static class RavenJobStoreExtensions
    {
        internal static string DocumentId<T>(this Key<T> key) => key.Name + "/" + key.Group;
    }
}