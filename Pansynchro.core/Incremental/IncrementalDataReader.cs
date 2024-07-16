using System;
using System.Collections.Generic;
using System.Data;

namespace Pansynchro.Core.Incremental
{
	public abstract class IncrementalDataReader : IDataReader
	{
		protected readonly IDataReader _reader;
		protected internal long _rows;
		internal readonly Dictionary<long, string?> _bookmarks = new();

		protected IncrementalDataReader(IDataReader reader, int bookmarkLength)
		{
			_reader = reader;
			BookmarkLength = bookmarkLength;
		}

		public int BookmarkLength { get; }

		protected abstract int HiddenColumns { get; }

		public abstract UpdateRowType UpdateType { get; }

		public abstract IEnumerable<int> AffectedColumns { get; }

		public string? Bookmark(long row) => _bookmarks[row];

		protected abstract void PrepareBookmark();

		protected abstract string? SaveBookmark();

		public object this[int i] => _reader[i + HiddenColumns];

		public object this[string name] => _reader[name];

		public int Depth => _reader.Depth;

		public bool IsClosed => _reader.IsClosed;

		public int RecordsAffected => _reader.RecordsAffected;

		public int FieldCount => _reader.FieldCount - HiddenColumns;

		public void Close()
		{
			_reader.Close();
		}

		public bool GetBoolean(int i)
		{
			return _reader.GetBoolean(i + HiddenColumns);
		}

		public byte GetByte(int i)
		{
			return _reader.GetByte(i + HiddenColumns);
		}

		public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
		{
			return _reader.GetBytes(i + HiddenColumns, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return _reader.GetChar(i + HiddenColumns);
		}

		public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
		{
			return _reader.GetChars(i + HiddenColumns, fieldoffset, buffer, bufferoffset, length);
		}

		public IDataReader GetData(int i)
		{
			return _reader.GetData(i + HiddenColumns);
		}

		public string GetDataTypeName(int i)
		{
			return _reader.GetDataTypeName(i + HiddenColumns);
		}

		public DateTime GetDateTime(int i)
		{
			return _reader.GetDateTime(i + HiddenColumns);
		}

		public decimal GetDecimal(int i)
		{
			return _reader.GetDecimal(i + HiddenColumns);
		}

		public double GetDouble(int i)
		{
			return _reader.GetDouble(i + HiddenColumns);
		}

		public Type GetFieldType(int i)
		{
			return _reader.GetFieldType(i + HiddenColumns);
		}

		public float GetFloat(int i)
		{
			return _reader.GetFloat(i + HiddenColumns);
		}

		public Guid GetGuid(int i)
		{
			return _reader.GetGuid(i + HiddenColumns);
		}

		public short GetInt16(int i)
		{
			return _reader.GetInt16(i + HiddenColumns);
		}

		public int GetInt32(int i)
		{
			return _reader.GetInt32(i + HiddenColumns);
		}

		public long GetInt64(int i)
		{
			return _reader.GetInt64(i + HiddenColumns);
		}

		public string GetName(int i)
		{
			return _reader.GetName(i + HiddenColumns);
		}

		public int GetOrdinal(string name)
		{
			return _reader.GetOrdinal(name) - HiddenColumns;
		}

		public DataTable? GetSchemaTable()
		{
			return _reader.GetSchemaTable();
		}

		public string GetString(int i)
		{
			return _reader.GetString(i + HiddenColumns);
		}

		public object GetValue(int i)
		{
			return _reader.GetValue(i + HiddenColumns);
		}

		public int GetValues(object[] values)
		{
			for (int i = 0; i < values.Length; ++i) {
				values[i] = GetValue(i);
			}
			return values.Length;
		}

		internal int GetValuesOffset(object[] values, int offset)
		{
			for (int i = 0; i < FieldCount; ++i) {
				values[i + offset] = GetValue(i);
			}
			return FieldCount;
		}

		public bool IsDBNull(int i)
		{
			return _reader.IsDBNull(i + HiddenColumns);
		}

		public bool NextResult()
		{
			return _reader.NextResult();
		}

		public bool Read()
		{
			var result = _reader.Read();
			if (result) {
				PrepareBookmark();
				++_rows;
			}
			if ((!result) || _rows % BookmarkLength == 0) {
				_bookmarks.Add(_rows, SaveBookmark());
			}
			return result;
		}

		public void Dispose()
		{
			_reader.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}