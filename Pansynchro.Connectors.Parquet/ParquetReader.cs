using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Parquet;

using PReader = Parquet.ParquetReader;
using DataColumn = Parquet.Data.DataColumn;

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
						yield return await ReadDataStream(source.GetStream(sName), stream);
					} else {
						stream.Dispose();
					}
				}
			}
		}

		public Task<DataStream> ReadStream(DataDictionary source, string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
			}
			var stream = source.GetStream(name);
			var readers = _source.GetDataAsync(name)
				.Select(async (Stream ds, CancellationToken t) => (await ReadDataStream(stream, ds)).Reader);
			return Task.FromResult<DataStream>(new(stream.Name, StreamSettings.None, new GroupingReader(readers)));
		}

		private static async Task<DataStream> ReadDataStream(StreamDefinition streamDef, Stream stream)
		{
			using var lStream = StreamHelper.SeekableStream(stream);
			var table = (await PReader.CreateAsync(lStream));
			return new DataStream(streamDef.Name, StreamSettings.None, new ParquetTableReader(table, streamDef));
		}

		public Task<Exception?> TestConnection() => Task.FromResult<Exception?>(null);

		public void Dispose()
		{
			GC.SuppressFinalize(this);
		}

		private class ParquetTableReader : IDataReader
		{
			private readonly PReader _table;
			private readonly KeyValuePair<string, int>[] _fields;
			private readonly Dictionary<string, int> _fieldDict;
			private DataColumn[] _values;
			private int _groupIndex;
			private int _rowIndex = -1;
			private int _rowCount;
			private bool _closed = false;

			public ParquetTableReader(PReader table, StreamDefinition streamDef)
			{
				_table = table;
				var schema = table.Schema.GetDataFields().Select(df => df.Name).ToArray();
				_fields = streamDef.Fields
					.Select(f => KeyValuePair.Create(f.Name, Array.IndexOf(schema, f.Name)))
					.ToArray();
				_fieldDict = new Dictionary<string, int>(_fields);
				_values = table.ReadEntireRowGroupAsync().GetAwaiter().GetResult();
				_rowCount = (int)_table.RowGroups[0].RowCount;
			}

			public object this[int i] => _values[_fields[i].Value].Data.GetValue(_rowIndex)!;

			public object this[string name] => _values[_fieldDict[name]].Data.GetValue(_rowIndex)!;

			public int Depth => 0;

			public bool IsClosed => _closed;

			public int RecordsAffected => _table.RowGroups.Sum(g => (int)g.RowCount);

			public int FieldCount => _fields.Length;

			public void Close() => _closed = true;

			public bool GetBoolean(int i) => (bool)this[i];

			public byte GetByte(int i) => (byte)this[i];

			public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
			{
				throw new NotImplementedException();
			}

			public char GetChar(int i) => (char)this[i];

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

			public DateTime GetDateTime(int i) => ((DateTimeOffset)this[i]).DateTime;

			public decimal GetDecimal(int i) => (decimal)this[i];

			public double GetDouble(int i) => (double)this[i];

			[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
			public Type GetFieldType(int i) => _table.Schema.GetDataFields()[_fields[i].Value].ClrType;

			public float GetFloat(int i) => (float)this[i];

			public Guid GetGuid(int i) => (Guid)this[i];

			public short GetInt16(int i) => (short)this[i];

			public int GetInt32(int i) => (int)this[i];

			public long GetInt64(int i) => (long)this[i];

			public string GetName(int i) => _fields[i].Key;

			public int GetOrdinal(string name)
			{
				throw new NotImplementedException();
			}

			public DataTable? GetSchemaTable()
			{
				return null;
			}

			public string GetString(int i) => (string)this[i];

			public object GetValue(int i) => this[i];

			public int GetValues(object[] values)
			{
				var result = Math.Min(values.Length, _values.Length);
				for (int i = 0; i < result; ++i) {
					values[i] = this[i];
				}
				return result;
			}

			public bool IsDBNull(int i) => this[i] == null;

			public bool NextResult() => Read();

			public bool Read()
			{
				++_rowIndex;
				if (_rowIndex < _rowCount) {
					return true;
				}
				if (_groupIndex < _table.RowGroups.Count) {
					++_groupIndex;
					_values = _table.ReadEntireRowGroupAsync(_groupIndex).GetAwaiter().GetResult();
					_rowCount = (int)_table.RowGroups[0].RowCount;
					_rowIndex = 0;
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
