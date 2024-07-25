using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

using Pansynchro.Core.Incremental;

namespace Pansynchro.SQL
{
	internal class SqlAuditReader : IncrementalDataReader
	{
		private readonly int _auditColumnIdx;
		private readonly int[] _fields;

		public SqlAuditReader(DbDataReader reader, string auditColumnName) : base(reader, 1000)
		{
			this._auditColumnIdx = _reader.GetOrdinal(auditColumnName);
			_fields = Enumerable.Range(0, reader.FieldCount).ToArray();
		}

		public override UpdateRowType UpdateType => UpdateRowType.Insert;

		public override IEnumerable<int> AffectedColumns => _fields;

		protected override int HiddenColumns => 0;

		protected override void PrepareBookmark() { }

		protected override string? SaveBookmark()
		{
			var result = _reader.GetValue(_auditColumnIdx);
			if (result is DateTime || result is DateTimeOffset) {
				return '\'' + result.ToString() + '\'';
			}
			return result.ToString();
		}
	}
}