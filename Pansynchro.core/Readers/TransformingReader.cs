using System;
using System.Collections.Generic;
using System.Data;

namespace Pansynchro.Core.Readers
{
	internal class TransformingReader : ArrayReader
	{
		private readonly IDataReader _reader;
		private readonly IEnumerator<object?[]> _transformer;
		private List<string> _names = new();

		public TransformingReader(IDataReader reader, Func<IDataReader, IEnumerable<object?[]>> transformer, DataDict.StreamDefinition stream)
		{
			_reader = reader;
			_transformer = transformer(reader).GetEnumerator();
			for (int i = 0; i < stream.Fields.Length; ++i) {
				_nameMap.Add(stream.Fields[i].Name, i);
				_names.Add(stream.Fields[i].Name);
			}
		}

		public override int RecordsAffected => _reader.RecordsAffected;

		public override int FieldCount => _names.Count;

		public override void Close()
		{
			base.Close();
			_reader.Close();
		}

		public override string GetName(int i) => _names[i];

		public override int GetOrdinal(string name) => _nameMap[name];

		public override bool Read()
		{
			var result = _transformer.MoveNext();
			if (result) {
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