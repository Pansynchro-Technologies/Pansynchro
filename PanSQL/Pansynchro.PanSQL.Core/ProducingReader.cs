using System;
using System.Collections.Generic;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.PanSQL.Core
{
	public class ProducingReader : ArrayReader
	{
		private readonly IEnumerator<object?[]> _producer;
		private readonly List<string> _names = new();

		public ProducingReader(IEnumerable<object?[]> producer, StreamDefinition stream)
		{
			_producer = producer.GetEnumerator();
			for (int i = 0; i < stream.Fields.Length; ++i) {
				_nameMap.Add(stream.Fields[i].Name, i);
				_names.Add(stream.Fields[i].Name);
			}
		}

		public override int RecordsAffected => -1;

		public override int FieldCount => _names.Count;

		public override string GetName(int i) => _names[i];

		public override int GetOrdinal(string name) => _nameMap[name];

		public override bool Read()
		{
			var result = _producer.MoveNext();
			if (result) {
				_buffer = _producer.Current!;
			}
			return result;
		}

		public override void Dispose()
		{
			_producer.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}