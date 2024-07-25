using System;
using System.Collections.Generic;

namespace Pansynchro.Core.Connectors
{
	public delegate bool ConnectionStringProcessingFunc(ref string connectionString);

	public static class ConnectionStringProcessor
	{
		private static readonly Dictionary<string, ConnectionStringProcessingFunc> _processors = new()
		{
			{"env", EnvProcessor}
		};

		private static ConnectionStringProcessingFunc? GetProcessor(string name)
			=> _processors.TryGetValue(name, out var result) ? result : null;

		public static void RegisterProcessor(string name, ConnectionStringProcessingFunc value)
			=> _processors.Add(name, value);

		internal static bool Process(ref string value)
		{
			var data = value[2..^1].Split(':');
			if (data.Length != 2) {
				return false;
			}
			var proc = GetProcessor(data[0].ToLowerInvariant());
			if (proc == null) {
				return false;
			}
			var result = proc(ref data[1]);
			if (result) {
				value = data[1];
			}
			return result;
		}

		private static bool EnvProcessor(ref string input)
		{
			var result = Environment.GetEnvironmentVariable(input);
			if (result != null) {
				input = result;
				return true;
			}
			return false;
		}
	}
}
