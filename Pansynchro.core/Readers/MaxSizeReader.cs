using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Pansynchro.Core.Readers
{
	internal class MaxSizeReader : IDataReader
	{
		private readonly IDataReader _source;
		private readonly int _throttle;
		private int _row = 0;

		public MaxSizeReader(IDataReader source, int throttle)
		{
			_source = source;
			_throttle = throttle;
		}

		public object this[int i] => ((IDataRecord)_source)[i];

		public object this[string name] => ((IDataRecord)_source)[name];

		public int Depth => _source.Depth;

		public bool IsClosed => _source.IsClosed;

		public int RecordsAffected => Math.Max(_source.RecordsAffected, _throttle);

		public int FieldCount => _source.FieldCount;

		public void Close()
		{
			_source.Close();
		}

		public void Dispose()
		{
			_source.Dispose();
		}

		public bool GetBoolean(int i)
		{
			return _source.GetBoolean(i);
		}

		public byte GetByte(int i)
		{
			return _source.GetByte(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
		{
			return _source.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return _source.GetChar(i);
		}

		public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
		{
			return _source.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		}

		public IDataReader GetData(int i)
		{
			return _source.GetData(i);
		}

		public string GetDataTypeName(int i)
		{
			return _source.GetDataTypeName(i);
		}

		public DateTime GetDateTime(int i)
		{
			return _source.GetDateTime(i);
		}

		public decimal GetDecimal(int i)
		{
			return _source.GetDecimal(i);
		}

		public double GetDouble(int i)
		{
			return _source.GetDouble(i);
		}

		[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
		public Type GetFieldType(int i)
		{
			return _source.GetFieldType(i);
		}

		public float GetFloat(int i)
		{
			return _source.GetFloat(i);
		}

		public Guid GetGuid(int i)
		{
			return _source.GetGuid(i);
		}

		public short GetInt16(int i)
		{
			return _source.GetInt16(i);
		}

		public int GetInt32(int i)
		{
			return _source.GetInt32(i);
		}

		public long GetInt64(int i)
		{
			return _source.GetInt64(i);
		}

		public string GetName(int i)
		{
			return _source.GetName(i);
		}

		public int GetOrdinal(string name)
		{
			return _source.GetOrdinal(name);
		}

		public DataTable? GetSchemaTable()
		{
			return _source.GetSchemaTable();
		}

		public string GetString(int i)
		{
			return _source.GetString(i);
		}

		public object GetValue(int i)
		{
			return _source.GetValue(i);
		}

		public int GetValues(object[] values)
		{
			return _source.GetValues(values);
		}

		public bool IsDBNull(int i)
		{
			return _source.IsDBNull(i);
		}

		public bool NextResult()
		{
			return Read();
		}

		public bool Read()
		{
			++_row;
			if (_row >= _throttle) {
				return false;
			}
			return _source.Read();
		}
	}
}
