using Microsoft.Extensions.Logging;
using Pansynchro.Core.EventsSystem;
using System;

namespace Pansynchro.Core.Logs.ColorConsoleLogger;

public sealed class ColorConsoleLogger : ILogger
{
	private readonly string _name;
	private readonly Func<ColorConsoleLoggerConfiguration> _getCurrentConfig;

	public ColorConsoleLogger(string name, Func<ColorConsoleLoggerConfiguration> getCurrentConfig)
	{
		this._name = name;
		this._getCurrentConfig = getCurrentConfig;
	}
	public IDisposable? BeginScope<TState>(TState state) where TState : notnull => default!;

	public bool IsEnabled(LogLevel logLevel) =>
		_getCurrentConfig().LogLevelToColorMap.ContainsKey(logLevel);

	private static string MessageFormatter(string? message, Exception? error)
	{
		if (message == null) {
			if (error == null) {
				return "Empty Error";
			}
			return error.ToString();
		}
		if (error == null) {
			return message;
		}
		return $"{message}\n{error.ToString()}";
	}

	public void Log<TState>(
		LogLevel logLevel,
		EventId eventId,
		TState state,
		Exception? exception,
		Func<TState, Exception?, string> formatter)
	{
		if (!EventLog.Instance.ConsoleOutput) {
			return;
		}

		if (!IsEnabled(logLevel)) {
			return;
		}

		ColorConsoleLoggerConfiguration config = _getCurrentConfig();
		if (config.EventId == 0 || config.EventId == eventId.Id) {
			var originalColor = Console.ForegroundColor;
			ConsoleColor foregroundColor;
			if (!config.LogLevelToColorMap.TryGetValue(logLevel, out foregroundColor))
				foregroundColor = ConsoleColor.White;
			var logLevelStr = logLevel.ToString().Substring(0, Math.Min(logLevel.ToString().Length, 4)).ToUpper();

			Console.Write($"{DateTime.Now}-{_name}: ");

			Console.ForegroundColor = foregroundColor;
			Console.Write($"[{logLevelStr,-4}] {ColorConsoleLogger.MessageFormatter(state?.ToString(), exception)}");

			Console.ForegroundColor = originalColor;
			Console.WriteLine();
		}
	}
}
