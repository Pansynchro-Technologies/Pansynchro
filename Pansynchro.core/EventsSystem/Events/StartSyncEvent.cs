namespace Pansynchro.Core.EventsSystem.Events;

public class StartSyncEvent : EventBase
{
	public StartSyncEvent(string? message = null) : base(message)
	{
	}
}
