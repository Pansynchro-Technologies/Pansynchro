using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Parquet.Data.Rows;
using PReader = Parquet.ParquetReader;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.Parquet
{
	public class ParquetReader : IReader, ISourcedConnector, IRandomStreamReader
	{
		private IDataSource? _source;

		public ParquetReader(string config) { }

		public void SetDataSource(IDataSource source) => _source = source;

		public IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
			}
			return DataStream.CombineStreamsByName(Impl());

			async IAsyncEnumerable<DataStream> Impl()
			{
				await foreach (var (sName, stream) in _source.GetDataAsync()) {
					if (source.HasStream(sName)) {
						yield return ReadDataStream(source.GetStream(sName), stream);
					} else {
						stream.Dispose();
					}
				}
			}
		}

		public Task<IDataReader> ReadStream(DataDictionary source, string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
			}
			var stream = source.GetStream(name);
			var readers = _source.GetDataAsync(name)
				.Select(ds => ReadDataStream(stream, ds).Reader)
				.ToEnumerable();
			return Task.FromResult<IDataReader>(new GroupingReader(readers));
		}

		private static DataStream ReadDataStream(StreamDefinition streamDef, Stream stream)
		{
			using var lStream = StreamHelper.SeekableStream(stream);
			var table = PReader.ReadTableFromStream(lStream);
			return new DataStream(streamDef.Name, StreamSettings.None, new ParquetTableReader(table, streamDef));
		}

		public Task<Exception?> TestConnection() => Task.FromResult<Exception?>(null);

		public void Dispose()
		{
			GC.SuppressFinalize(this);
		}

		private class ParquetTableReader : IDataReader
		{
			private readonly Table _table;
			private readonly KeyValuePair<string, int>[] _fields;
			private readonly Dictionary<string, int> _fieldDict;
			private int _index = -1;
			private bool _closed = false;

			public ParquetTableReader(Table table, StreamDefinition streamDef)
			{
				_table = table;
				var schema = table.Schema.GetDataFields().Select(df => df.Name).ToArray();
				_fields = streamDef.Fields
					.Select(f => KeyValuePair.Create(f.Name, Array.IndexOf(schema, f.Name)))
					.ToArray();
				_fieldDict = new Dictionary<string, int>(_fields);
			}

			public object this[int i] => _table[_index][_fields[i].Value];

			public object this[string name] => _table[_index][_fieldDict[name]];

			public int Depth => 0;

			public bool IsClosed => _closed;

			public int RecordsAffected => _table.Count;

			public int FieldCount => _fields.Length;

			public void Close() => _closed = true;

			public bool GetBoolean(int i) => _table[_index].GetBoolean(_fields[i].Value);

			public byte GetByte(int i) => _table[_index].Get<byte>(_fields[i].Value);

			public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public char GetChar(int i) => _table[_index].Get<char>(_fields[i].Value);

			public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public IDataReader GetData(int i)
			{
				throw new NotImplementedException();
			}

			public string GetDataTypeName(int i)
			{
				throw new NotImplementedException();
			}

			public DateTime GetDateTime(int i) => _table[_index].GetDateTimeOffset(_fields[i].Value).DateTime;

			public decimal GetDecimal(int i) => _table[_index].Get<Decimal>(_fields[i].Value);

			public double GetDouble(int i) => _table[_index].GetDouble(_fields[i].Value);

			[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
			public Type GetFieldType(int i) => _table.Schema.GetDataFields()[_fields[i].Value].ClrType;

			public float GetFloat(int i) => _table[_index].GetFloat(_fields[i].Value);

			public Guid GetGuid(int i) => _table[_index].Get<Guid>(_fields[i].Value);

			public short GetInt16(int i) => _table[_index].Get<short>(_fields[i].Value);

			public int GetInt32(int i) => _table[_index].GetInt(_fields[i].Value);

			public long GetInt64(int i) => _table[_index].GetLong(_fields[i].Value);

			public string GetName(int i) => _fields[i].Key;

			public int GetOrdinal(string name)
			{
				throw new NotImplementedException();
			}

			public DataTable? GetSchemaTable()
			{
				return null;
			}

			public string GetString(int i) => _table[_index].GetString(_fields[i].Value);

			public object GetValue(int i) => this[i];

			public int GetValues(object[] values)
			{
				var data = _table[_index].Values;
				var result = Math.Min(values.Length, data.Length);
				data.CopyTo(values, 0);
				return result;
			}

			public bool IsDBNull(int i) => _table[_index].IsNullAt(_fields[i].Value);

			public bool NextResult() => Read();

			public bool Read()
			{
				++_index;
				if (_index < _table.Count) {
					return true;
				}
				Close();
				return false;
			}

			public void Dispose()
			{
				Close();
				GC.SuppressFinalize(this);
			}
		}
	}
}
