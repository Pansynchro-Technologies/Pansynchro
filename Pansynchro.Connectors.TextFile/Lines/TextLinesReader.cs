using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.TextFile.Lines
{
	class TextLinesReader : IReader, ISourcedConnector, IRandomStreamReader
	{
		private IDataSource? _source;
		private readonly string _config;

		public TextLinesReader(string config)
		{
			_config = config;
		}

		IAsyncEnumerable<DataStream> IReader.ReadFrom(DataDictionary source)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling ReadFrom");
			}
			return DataStream.CombineStreamsByName(Impl());

			async IAsyncEnumerable<DataStream> Impl()
			{
				await foreach (var (name, reader) in _source.GetTextAsync()) {
					try {
						yield return new DataStream(new(null, name), StreamSettings.None, new FileLinesReader(name, reader));
					} finally {
						reader.Dispose();
					}
				}
			}
		}

		public Task<DataStream> ReadStream(DataDictionary source, string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling ReadStream");
			}
			var values = _source.GetTextAsync(name).Select(r => new FileLinesReader(name, r));
			var result = new GroupingReader(values);
			return Task.FromResult<DataStream>(new(StreamDescription.Parse(name), StreamSettings.None, result));
		}

		void ISourcedConnector.SetDataSource(IDataSource source) => _source = source;

		async Task<Exception?> IReader.TestConnection()
		{
			if (_source == null) {
				return new DataException("No data source has been set.");
			}
			try {
				await foreach (var (name, stream) in _source.GetDataAsync()) {
					break;
				}
			} catch (Exception e) {
				return e;
			}
			return null;
		}

		void IDisposable.Dispose()
		{
			GC.SuppressFinalize(this);
		}
	}

	internal class FileLinesReader : ArrayReader
	{
		private static readonly string[] NAMES = new string[] { "Name", "Line", "Value" };

		private readonly TextReader _reader;

		public FileLinesReader(string name, TextReader reader)
		{
			_reader = reader;
			_buffer = new object?[] { name, 0, null }!;
		}

		public override int RecordsAffected => throw new NotImplementedException();

		public override string GetName(int i) => NAMES[i];

		public override int GetOrdinal(string name) => Array.IndexOf(NAMES, name);

		public override bool Read()
		{
			_buffer[1] = (int)_buffer[1] + 1;
			_buffer[2] = _reader.ReadLine()!;
			return _buffer[2] != null;
		}

		public override void Dispose()
		{ }
	}
}
