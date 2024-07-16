namespace Pansynchro.Core.EventsSystem.Events;
public class EndSyncStreamEvent : StreamEvent
{
	public EndSyncStreamEvent(string streamName) : base(streamName, null)
	{
	}
}

