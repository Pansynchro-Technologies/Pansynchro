using Microsoft.Extensions.Logging;
using System;

namespace Pansynchro.Core.EventsSystem;
public interface IEvent
{
	string Name { get; }
	LogLevel LogLevel { get; }
	DateTime FireOn { get; }
}

public interface IStreamEvent : IEvent
{
	string? StreamName { get; }
}