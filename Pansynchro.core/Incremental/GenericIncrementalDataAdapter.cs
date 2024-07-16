using System;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.Readers;

namespace Pansynchro.Core.Incremental
{
	internal class GenericIncrementalDataAdapter : ArrayReader
	{
		private readonly IncrementalDataReader _reader;

		public GenericIncrementalDataAdapter(IncrementalDataReader reader)
		{
			_reader = reader;
			_buffer = new object[reader.FieldCount + 3];
		}

		public override int RecordsAffected => _reader.RecordsAffected;

		public override string GetName(int i)
			=> i switch {
				0 => IncrementalHelper.BOOKMARK_FIELD_NAME,
				1 => IncrementalHelper.TYPE_FIELD_NAME,
				2 => IncrementalHelper.AFFECTED_FIELD_NAME,
				_ => _reader.GetName(i - 3),
			};

		private int _bookmarkCount;

		public override bool Read()
		{
			if (!_reader.Read()) {
				return false;
			}
			_reader.GetValuesOffset(_buffer, 3);
			if (_reader._bookmarks.Count > _bookmarkCount) {
				_buffer[0] = _reader.Bookmark(_reader._rows)!;
				_bookmarkCount = _reader._bookmarks.Count;
			} else {
				_buffer[0] = DBNull.Value;
			}
			_buffer[1] = (int)_reader.UpdateType;
			_buffer[2] = string.Join(',', _reader.AffectedColumns);
			return true;
		}

		public override void Dispose()
		{
			_reader.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
