using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pansynchro.Core.Logs.ColorConsoleLogger;

public sealed class ColorConsoleLoggerConfiguration
{
	public int EventId { get; set; }

	public Dictionary<LogLevel, ConsoleColor> LogLevelToColorMap { get; set; } = new() {
		[LogLevel.Information] = ConsoleColor.Green,
		[LogLevel.Trace] = ConsoleColor.Blue,
		[LogLevel.Debug] = ConsoleColor.White,
		[LogLevel.Warning] = ConsoleColor.Yellow,
		[LogLevel.Error] = ConsoleColor.Red,
		[LogLevel.Critical] = ConsoleColor.DarkRed,
		[LogLevel.None] = ConsoleColor.White
	};
}
