using Pansynchro.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Pansynchro.Core.Incremental
{
	public class GenericIncrementalDataReader : IncrementalDataReader
	{
		public GenericIncrementalDataReader(IDataReader reader, int bookmarkLength) : base(reader, bookmarkLength)
		{
			if (!IsIncrementalData(reader)) {
				throw new ArgumentException("Underlying data reader is not a Pansynchro incremental data set");
			}
		}

		public static bool IsIncrementalData(IDataReader reader)
		{
			return reader.GetName(0) == IncrementalHelper.BOOKMARK_FIELD_NAME
				&& reader.GetName(1) == IncrementalHelper.TYPE_FIELD_NAME
				&& reader.GetName(2) == IncrementalHelper.AFFECTED_FIELD_NAME;
		}

		public override UpdateRowType UpdateType => (UpdateRowType)_reader.GetInt32(1);

		public override IEnumerable<int> AffectedColumns => _reader.GetString(2).Split(',').Select(int.Parse);

		protected override int HiddenColumns => 3;

		private string? _lastBookmark;

		protected override void PrepareBookmark()
		{
			_lastBookmark = _reader.IsDBNull(0) ? null : _reader.GetString(0);
		}

		protected override string? SaveBookmark() => _lastBookmark;
	}
}
