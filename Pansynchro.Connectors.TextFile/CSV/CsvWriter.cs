using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.TextFile.CSV
{
	public class CsvWriter : IWriter, ISinkConnector
	{
		private readonly CsvConfigurator _config;
		private IDataSink? _sink;
		private readonly char[] _escapes;
		private readonly string _quoteOne;
		private readonly string _quoteTwo;

		public CsvWriter(string config)
		{
			_config = new CsvConfigurator(config);
			_escapes = new char[] { '\r', '\n', _config.QuoteChar };
			_quoteOne = new string(_config.QuoteChar, 1);
			_quoteTwo = new string(_config.QuoteChar, 2);
		}

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			if (_sink == null) {
				throw new DataException("Must call SetDataSink before calling Sync");
			}
			EventLog.Instance.AddStartSyncEvent();
			await foreach (var (name, settings, stream) in streams) {
				try {
					using var tw = await _sink!.WriteText(name.ToString());
					Write(stream, tw);
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, name);
					if (!ErrorManager.ContinueOnError)
						throw;
				} finally {
					stream.Dispose();
				}
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		private void Write(IDataReader reader, TextWriter tw)
		{
			for (int i = 0; i < reader.FieldCount; ++i) {
				tw.Write(reader.GetName(i));
				if (i < reader.FieldCount - 1) {
					tw.Write(_config.Delimiter);
				}
			}
			tw.WriteLine();
			while (reader.Read()) {
				WriteRow(reader, tw);
			}
		}

		private void WriteRow(IDataReader reader, TextWriter tw)
		{
			for (int i = 0; i < reader.FieldCount; ++i) {
				var value = reader.GetValue(i);
				switch (value) {
					case null:
					case DBNull:
						break;
					case int:
					case long:
					case float:
					case double:
					case decimal:
					case Guid:
					case DateTime:
					case DateTimeOffset:
						tw.Write(value.ToString());
						break;
					default:
						WriteString(value.ToString()!, tw);
						break;
				}
				if (i < reader.FieldCount - 1) {
					tw.Write(_config.Delimiter);
				}
			}
			tw.WriteLine();
		}

		private void WriteString(string s, TextWriter tw)
		{
			if (s.IndexOfAny(_escapes) == -1) {
				tw.Write(s);
			} else {
				tw.Write(_config.QuoteChar);
				tw.Write(s.Replace(_quoteOne, _quoteTwo));
				tw.Write(_config.QuoteChar);
			}
		}

		public void SetDataSink(IDataSink sink)
		{
			_sink = sink;
		}

		public void Dispose()
		{
			(_sink as IDisposable)?.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
