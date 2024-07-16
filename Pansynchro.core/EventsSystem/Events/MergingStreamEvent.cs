namespace Pansynchro.Core.EventsSystem.Events;
public class MergingStreamEvent : StreamEvent
{
	public MergingStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
