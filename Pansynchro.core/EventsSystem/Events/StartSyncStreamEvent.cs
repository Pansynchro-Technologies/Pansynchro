namespace Pansynchro.Core.EventsSystem.Events;
public class StartSyncStreamEvent : StreamEvent
{
	public StartSyncStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
