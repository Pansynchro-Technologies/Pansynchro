using Pansynchro.Core.EventsSystem;

namespace Microsoft.Extensions.Logging;

public static class LoggerExtensions
{
	public static void LogEvent(this ILogger logger, IEvent @event)
	{
		logger.Log(@event.LogLevel, @event.ToString());
	}
}
