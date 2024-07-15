using Microsoft.Extensions.Logging;
using Pansynchro.Core.EventsSystem.Events;
using Pansynchro.Core.Logs.ColorConsoleLogger;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem;

public class EventLog
{
	private EventLog() { }

	public static EventLog Instance { get; } = new EventLog();

	private bool consoleOutput = true;
	private List<IEvent> events = new List<IEvent>();

	private ILogger _logger = new ColorConsoleLogger("Pansynchro", () => new ColorConsoleLoggerConfiguration());

	public bool ConsoleOutput { get { return consoleOutput; } }

	public void DisableConsoleOutput() { consoleOutput = false; }
	public void EnableConsoleOutput() { consoleOutput = true; }

	public void SetDotNetLogger(ILogger logger) { _logger = logger; }

	public IEnumerable<IEvent> GetEvents() { return events.ToArray(); }

	public void AddEvent(IEvent @event)
	{
		events.Add(@event);
		_logger?.LogEvent(@event);

	}
}
