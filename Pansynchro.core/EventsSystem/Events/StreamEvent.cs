namespace Pansynchro.Core.EventsSystem.Events;
public class StreamEvent : EventBase, IStreamEvent
{
	public StreamEvent(string streamName, string? message) : base(message)
	{
		StreamName = streamName;
	}

	public string StreamName { get; }
}
