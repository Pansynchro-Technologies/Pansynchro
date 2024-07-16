namespace Pansynchro.Core.EventsSystem.Events;
internal class WarningEvent : EventBase
{
	public WarningEvent(string message) : base(message)
	{
		LogLevel = Microsoft.Extensions.Logging.LogLevel.Warning;
	}
}