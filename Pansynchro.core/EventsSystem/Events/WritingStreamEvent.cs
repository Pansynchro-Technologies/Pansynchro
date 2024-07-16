namespace Pansynchro.Core.EventsSystem.Events;
public class WritingStreamEvent : StreamEvent
{
	public WritingStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
