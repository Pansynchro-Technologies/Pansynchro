namespace Pansynchro.Core.Incremental
{
    public enum IncrementalStrategy
    {
        None,
        Cdc,
        Column,
        AuditTable
    }
}
