using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Avro;
using Avro.File;
using Avro.Generic;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.Avro
{
	public class AvroReader : IReader, ISourcedConnector, IRandomStreamReader
	{
		private IDataSource? _source;

		public void SetDataSource(IDataSource source)
		{
			_source = source;
		}

		public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling ReadFrom");
			}
			await foreach (var (_, stream) in _source.GetDataAsync()) {
				using var reader = DataFileReader<GenericRecord>.OpenReader(stream);
				var schema = (RecordSchema)reader.GetSchema();
				yield return new DataStream(new(schema.Namespace, schema.Name), StreamSettings.None, new AvroDataReader(reader));
			}
		}

		public Task<Exception?> TestConnection() => Task.FromResult<Exception?>(null);

		public Task<IDataReader> ReadStream(DataDictionary source, string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling ReadStream");
			}
			var stream = source.GetStream(name);
			var readers = _source.GetDataAsync(name)
				.Select(s => new AvroDataReader(DataFileReader<GenericRecord>.OpenReader(s)))
				.ToEnumerable();
			return Task.FromResult<IDataReader>(new GroupingReader(readers));
		}

		public void Dispose()
		{
			(_source as IDisposable)?.Dispose();
			GC.SuppressFinalize(this);
		}

		private class AvroDataReader : ArrayReader
		{
			private readonly IFileReader<GenericRecord> _reader;
			private readonly RecordSchema _schema;
			private IEnumerator<GenericRecord> _enumerator = null!;
			private readonly Func<GenericRecord, object[]> _interpreter;

			public AvroDataReader(IFileReader<GenericRecord> reader)
			{
				_reader = reader;
				_schema = (RecordSchema)reader.GetSchema();
				_buffer = new object[_schema.Fields.Count];
				for (int i = 0; i < _schema.Fields.Count; i++) {
					_nameMap[_schema.Fields[i].Name] = i;
				}
				_interpreter = BuildInterpreter(_schema);
				_enumerator = reader.NextEntries.GetEnumerator();
			}

			public override int RecordsAffected => throw new NotImplementedException();

			public override string GetName(int i) => _schema.Fields[i].Name;

			public override bool Read()
			{
				if (!_enumerator.MoveNext()) {
					return false;
				}
				_buffer = _interpreter(_enumerator.Current);
				return true;
			}

			private Func<GenericRecord, object[]> BuildInterpreter(RecordSchema schema)
			{
				var indices = schema.Fields.IndexWhere(f => f.Schema is EnumSchema).ToArray();
				if (indices.Length == 0) {
					return this.ReadRecord;
				}
				Action<object[]> interpreter = null!;
				foreach (var i in indices) {
					interpreter += BuildEnumInterpreter(i);
				}
				return r => {
					var result = ReadRecord(r);
					interpreter(result);
					return result;
				};
			}

			private static Action<object[]> BuildEnumInterpreter(int i) =>
				arr => {
					var e = (GenericEnum)arr[i];
					arr[i] = e.Schema.Ordinal(e.Value);
				};

			private object[] ReadRecord(GenericRecord item)
			{
				var result = new object[_schema.Fields.Count];
				for (var i = 0; i < _schema.Fields.Count; i++) {
					result[i] = item.GetValue(i);
				}
				return result;
			}

			public override void Dispose()
			{
				_reader.Dispose();
				GC.SuppressFinalize(this);
			}
		}
	}
}