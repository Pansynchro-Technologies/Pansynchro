namespace Pansynchro.Core.Incremental
{
	public enum IncrementalStrategy
	{
		None,
		CdcByTable,
		Column,
		ChangeTracking,
		CdcFull,
	}
}
