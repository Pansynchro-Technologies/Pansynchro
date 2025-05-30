using System;
using System.Collections.Generic;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.Core.Transformations
{
	public class EnumerableArrayReader : ArrayReader
	{
		private readonly IEnumerator<object?[]> _source;
		private readonly string[] _names;

		public EnumerableArrayReader(IEnumerable<object?[]> source, StreamDefinition stream)
		{
			_source = source.GetEnumerator();
			_names = stream.NameList;
		}

		public override int RecordsAffected => throw new NotImplementedException();

		public override string GetName(int i) => _names[i];

		public override int GetOrdinal(string name) => Array.IndexOf(_names, name);

		public override bool Read()
		{
			if (_source.MoveNext()) {
				_buffer = _source.Current!;
				return true;
			}
			return false;
		}

		public override void Dispose()
		{
			_source.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
