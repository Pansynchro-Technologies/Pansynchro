using System;

namespace Pansynchro.Core.EventsSystem.Events;
public class ErrorEvent : ExceptionEvent, IStreamEvent
{
	public string? StreamName { get; }
	public ErrorEvent(Exception? ex, StreamDescription? stream, string? message) : base(ex, message)
	{
		StreamName = stream?.ToString();
	}
}
