using System;
using System.Collections.Generic;
using System.Data;

using ExcelDataReader;

namespace Pansynchro.Connectors.Excel
{
	internal class ExcelReaderWrapper : IDataReader
	{
		private readonly IExcelDataReader _reader;
		private readonly List<string> _names = new();

		public ExcelReaderWrapper(IExcelDataReader excelReader)
		{
			_reader = excelReader;
			if (_reader.Read()) {
				for (int i = 0; i < _reader.FieldCount; i++) {
					_names.Add(_reader.GetString(i) ?? ExcelColumnName(i));
				}
			}
		}

		private static string ExcelColumnName(int i)
		{
			if (i < 26) {
				return new string((char)((int)'A' + i), 1);
			}
			return new string((char)((int)'A' + i % 26), i / 26);
		}

		public object this[int i] => _reader[i];

		public object this[string name] => _reader[_names.IndexOf(name)];

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
			return _names[i];
		}

		public int GetOrdinal(string name)
		{
			return _names.IndexOf(name);
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
			var fc = Math.Min(values.Length, _reader.FieldCount);
			for (int i = 0; i < fc; ++i) {
				values[i] = _reader.GetValue(i);
			}
			return fc;
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
			return _reader.Read();
		}

		public void Dispose()
		{ }
	}
}