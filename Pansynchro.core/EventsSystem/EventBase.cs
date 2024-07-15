using Microsoft.Extensions.Logging;
using Pansynchro.Core.EventsSystem.Events;
using System;
using System.Text;

namespace Pansynchro.Core.EventsSystem;
public abstract class EventBase : IEvent
{
	public string Name { get; }
	public DateTime FireOn { get; }
	public LogLevel LogLevel { get; protected set; } = LogLevel.Information;
	public string? Message { get; protected set; }

	protected EventBase(string? message)
	{
		Name = this.GetType().Name;
		FireOn = DateTime.Now;
		Message = message;
	}

	public override string ToString()
	{
		var text = new StringBuilder(Name);

		if (this is IStreamEvent streamEvent) {
			text.Append($" {streamEvent.StreamName}");
		}

		if (Message is not null) {
			text = text.Append($" {Message}");
		}

		if (this is ExceptionEvent exceptionEvent && exceptionEvent.Exception is not null) {
			text.AppendLine();
			text.Append(exceptionEvent.Exception.ToString());
		}
		return text.ToString();
	}
}
