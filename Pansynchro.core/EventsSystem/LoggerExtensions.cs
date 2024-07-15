using Microsoft.Extensions.Logging;
using Pansynchro.Core.EventsSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Extensions.Logging;

public static class LoggerExtensions
{
	public static void LogEvent(this ILogger logger, IEvent @event)
	{
		logger.Log(@event.LogLevel, @event.ToString());
	}
}
