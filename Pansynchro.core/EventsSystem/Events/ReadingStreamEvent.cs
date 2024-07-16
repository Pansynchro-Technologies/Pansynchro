namespace Pansynchro.Core.EventsSystem.Events;
public class ReadingStreamEvent : StreamEvent
{
	public ReadingStreamEvent(string streamName, string? message = null) : base(streamName, message)
	{
	}
}
