using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

using Pansynchro.Core.Streams;

namespace Pansynchro.Core.Readers
{
	public class SizePartitionReader : IDataReader
	{
		private readonly IDataReader _reader;
		private readonly MeteredStream _meter;
		private readonly long _sizeLimit;

		public bool Finished { get; private set; }

		public SizePartitionReader(IDataReader reader, MeteredStream meter, long sizeLimit)
		{
			_reader = reader;
			_meter = meter;
			_sizeLimit = sizeLimit;
		}

		public object this[int i] => ((IDataRecord)_reader)[i];

		public object this[string name] => ((IDataRecord)_reader)[name];

		public int Depth => _reader.Depth;

		public bool IsClosed => _reader.IsClosed;

		public int RecordsAffected => _reader.RecordsAffected;

		public int FieldCount => _reader.FieldCount;

		public void Close()
		{
			_reader.Close();
		}

		public bool GetBoolean(int i)
		{
			return _reader.GetBoolean(i);
		}

		public byte GetByte(int i)
		{
			return _reader.GetByte(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
		{
			return _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return _reader.GetChar(i);
		}

		public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
		{
			return _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		}

		public IDataReader GetData(int i)
		{
			return _reader.GetData(i);
		}

		public string GetDataTypeName(int i)
		{
			return _reader.GetDataTypeName(i);
		}

		public DateTime GetDateTime(int i)
		{
			return _reader.GetDateTime(i);
		}

		public decimal GetDecimal(int i)
		{
			return _reader.GetDecimal(i);
		}

		public double GetDouble(int i)
		{
			return _reader.GetDouble(i);
		}

		[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
		public Type GetFieldType(int i)
		{
			return _reader.GetFieldType(i);
		}

		public float GetFloat(int i)
		{
			return _reader.GetFloat(i);
		}

		public Guid GetGuid(int i)
		{
			return _reader.GetGuid(i);
		}

		public short GetInt16(int i)
		{
			return _reader.GetInt16(i);
		}

		public int GetInt32(int i)
		{
			return _reader.GetInt32(i);
		}

		public long GetInt64(int i)
		{
			return _reader.GetInt64(i);
		}

		public string GetName(int i)
		{
			return _reader.GetName(i);
		}

		public int GetOrdinal(string name)
		{
			return _reader.GetOrdinal(name);
		}

		public DataTable? GetSchemaTable()
		{
			return _reader.GetSchemaTable();
		}

		public string GetString(int i)
		{
			return _reader.GetString(i);
		}

		public object GetValue(int i)
		{
			return _reader.GetValue(i);
		}

		public int GetValues(object[] values)
		{
			return _reader.GetValues(values);
		}

		public bool IsDBNull(int i)
		{
			return _reader.IsDBNull(i);
		}

		public bool NextResult()
		{
			return _reader.NextResult();
		}

		public bool Read()
		{
			if (_meter.TotalBytesWritten >= _sizeLimit) {
				return false;
			}
			// the next two lines are slightly convoluted, but the intent is that:
			// 1) the return value of this method will be the same as that of _reader.Read()
			// 2) if _reader.Read() returns false, then _finished will be set to true and _reader will
			//    get disposed in the Dispose() call
			// 3) if we hit the meter limit, _finished will be false and _reader will *not* be disposed
			Finished = !_reader.Read();
			return !Finished;
		}

		public void Dispose()
		{
			if (Finished) {
				_reader.Dispose();
			}
			GC.SuppressFinalize(this);
		}
	}
}
