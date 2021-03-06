using System;
using System.Collections.Generic;
using System.Data;

namespace Pansynchro.Core.Readers
{
    internal class TransformingReader : ArrayReader
    {
        private readonly IDataReader _reader;
        private readonly IEnumerator<object?[]> _transformer;

        public TransformingReader(IDataReader reader, Func<IDataReader, IEnumerable<object?[]>> transformer)
        {
            _reader = reader;
            _transformer = transformer(reader).GetEnumerator();
            for (int i = 0; i < _reader.FieldCount; ++i)
            {
                _nameMap.Add(_reader.GetName(i), i);
            }
        }

        public override int RecordsAffected => _reader.RecordsAffected;

        public override void Close()
        {
            base.Close();
            _reader.Close();
        }

        public override string GetName(int i) => _reader.GetName(i);

        public override int GetOrdinal(string name) => _reader.GetOrdinal(name);

        public override bool Read()
        {
            var result = _transformer.MoveNext();
            if (result)
            {
                _buffer = _transformer.Current!;
            }
            return result;
        }

        public override void Dispose()
        {
            _transformer.Dispose();
            _reader.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}