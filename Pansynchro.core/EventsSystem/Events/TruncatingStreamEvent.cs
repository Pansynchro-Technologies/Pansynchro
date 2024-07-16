namespace Pansynchro.Core.EventsSystem.Events;

public class TruncatingStreamEvent : StreamEvent
{
	public TruncatingStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
