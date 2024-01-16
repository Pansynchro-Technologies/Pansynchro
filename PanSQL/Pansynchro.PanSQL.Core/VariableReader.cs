using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Pansynchro.PanSQL.Core
{
	public class VariableReader
	{
		private readonly Dictionary<string, JsonNode?>? _values;
		private List<string> _errors = new();

		public string? Result => _errors.Count == 0 ? null : string.Join(Environment.NewLine, _errors);

		public VariableReader(string[] args, bool required)
		{
			if (args.Length == 0 && required) {
				_errors.Add("No variables filename was provided.");
			} else if (args.Length > 0) {
				var filename = args[0];
				if (!File.Exists(filename)) {
					_errors.Add($"File '{filename}' was not found.");
					return;
				}
				var jsonText = File.ReadAllText(filename);
				try { 
					var json = JsonValue.Parse(jsonText);
					var variables = json as JsonObject;
					if (variables == null) {
						_errors.Add($"File '{filename}' is not a valid JSON file.");
					} else {
						_values = new Dictionary<string, JsonNode?>(variables, StringComparer.InvariantCultureIgnoreCase);
					}
				} catch (JsonException) {
					_errors.Add($"File '{filename}' is not a valid JSON file.");
				}
			}
		}

		public VariableReader ReadVar<T>(string name, out T result) where T: IParsable<T>
		{
			if (_values == null) {
				result = default!;
			} else if (_values.TryGetValue(name, out var value) && value != null) {
				var jVal = value.ToString();
				if (!T.TryParse(jVal, CultureInfo.InvariantCulture, out result)) {
					_errors.Add($"Script variable '{name}': '{value}' cannot be assigned to an integer.");
				}
			} else {
				result = default!;
				_errors.Add($"Script variable '{name}' does not exist in the JSON file.");
			}
			return this;
		}

		public VariableReader TryReadVar<T>(string name, ref T result) where T : IParsable<T>
		{
			if (_values != null) {
				if (_values.TryGetValue(name, out var value) && value != null) {
					var jVal = value.ToString();
					if (!T.TryParse(jVal, CultureInfo.InvariantCulture, out result)) {
						_errors.Add($"Script variable '{name}': '{value}' cannot be assigned to an integer.");
					}
				}
			}
			return this;
		}
	}
}
