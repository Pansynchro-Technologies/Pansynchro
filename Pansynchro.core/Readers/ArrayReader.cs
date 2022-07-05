using System;
using System.Collections.Generic;
using System.Data;

namespace Pansynchro.Core.Readers
{
    public abstract class ArrayReader : IDataReader
    {
        protected object[] _buffer = null!;

        public object this[int i] => _buffer[i];

        public object this[string name] => _buffer[_nameMap[name]];

        protected readonly Dictionary<string, int> _nameMap = new();

        public int Depth => 0;

        private bool _closed;
        public virtual void Close() => _closed = true;

        public bool IsClosed => _closed;

        public abstract int RecordsAffected { get; }
        public int FieldCount => _buffer.Length;

        public bool GetBoolean(int i) => (bool)_buffer[i];

        public byte GetByte(int i) => (byte)_buffer[i];

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i) => (char)_buffer[i];

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

        public DateTime GetDateTime(int i) => (DateTime)_buffer[i];

        public decimal GetDecimal(int i) => (decimal)_buffer[i];

        public double GetDouble(int i) => (double)_buffer[i];

        public virtual Type? GetFieldType(int i) => _buffer[i]?.GetType();

        public float GetFloat(int i) => (float)_buffer[i];

        public Guid GetGuid(int i) => (Guid)_buffer[i];

        public short GetInt16(int i) => (short)_buffer[i];

        public int GetInt32(int i) => (int)_buffer[i];

        public long GetInt64(int i) => (long)_buffer[i];

        public DataTable? GetSchemaTable() => null;

        public string GetString(int i) => (string)_buffer[i];

        public object GetValue(int i) => _buffer[i];

        public int GetValues(object[] values)
        {
            _buffer.CopyTo(values, 0);
            return _buffer.Length;
            //            throw new NotImplementedException();
        }

        public bool IsDBNull(int i) => _buffer[i] == null;

        public bool NextResult() => Read();
        public abstract bool Read();
        public abstract string GetName(int i);
        public virtual int GetOrdinal(string name) => _nameMap[name];
        public abstract void Dispose();
    }
}
