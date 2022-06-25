using System;
using System.Linq;

using Newtonsoft.Json.Linq;

using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.TextFile.JSON
{
    internal class JsonArrayReader : ArrayReader
    {
        private readonly JArray _arr;
        private readonly string _fieldName;
        private int _ptr = 0;

        public JsonArrayReader(JArray arr, string fieldName)
        {
            _arr = arr;
            _buffer = new object[1];
            _fieldName = fieldName;
        }

        public override int RecordsAffected => _buffer.Length;

        public override string GetName(int i) => _fieldName;

        public override int GetOrdinal(string name) => name == _fieldName ? 0 : -1;

        public override bool Read()
        {
            ++_ptr;
            if (_ptr >= _arr.Count) {
                return false;
            }
            _buffer[0] = _arr[_ptr];
            return true;
        }

        public override void Dispose() => GC.SuppressFinalize(this);
    }
}