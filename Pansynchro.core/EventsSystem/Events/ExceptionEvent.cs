using System;

namespace Pansynchro.Core.EventsSystem.Events;

public class ExceptionEvent : EventBase
{
	public Exception? Exception { get; }
	public ExceptionEvent(Exception? ex, string? message) : base(message)
	{
		Exception = ex;
		LogLevel = Microsoft.Extensions.Logging.LogLevel.Error;
	}
}
