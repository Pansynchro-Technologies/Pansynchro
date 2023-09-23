using System;
using System.Collections.Generic;
using System.IO;

namespace Pansynchro.Protocol
{
	public interface IRcfReader
	{
		void AddData(BinaryReader reader);
	}

	public class RcfReader<T> : IRcfReader
	{
		private List<T> _cache = new();

		private Func<BinaryReader, object> _reader;

		public RcfReader(Func<BinaryReader, object> reader)
		{
			_reader = reader;
		}

		public object Read(BinaryReader reader)
		{
			var idx = reader.Read7BitEncodedInt();
			return _cache[idx]!;
        }

		void IRcfReader.AddData(BinaryReader reader)
		{
			var count = reader.Read7BitEncodedInt();
			for (int i = 0; i < count; i++) {
				_cache.Add((T)_reader(reader));
			}
		}
    }

    public class NullableRcfReader<T> : IRcfReader
    {
        private List<T> _cache = new();

        private Func<BinaryReader, object> _reader;

        public NullableRcfReader(Func<BinaryReader, object> reader)
        {
            _reader = reader;
        }

        public object Read(BinaryReader reader)
        {
            var idx = reader.Read7BitEncodedInt();
            return idx == 0 ? DBNull.Value : _cache[idx - 1]!;
        }

        void IRcfReader.AddData(BinaryReader reader)
        {
            var count = reader.Read7BitEncodedInt();
            for (int i = 0; i < count; i++) {
                _cache.Add((T)_reader(reader));
            }
        }
    }
}
