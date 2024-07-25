namespace Pansynchro.Core.EventsSystem.Events;

public class InformationEvent : EventBase
{
	public InformationEvent(string message) : base(message)
	{
		LogLevel = Microsoft.Extensions.Logging.LogLevel.Information;
	}
}
