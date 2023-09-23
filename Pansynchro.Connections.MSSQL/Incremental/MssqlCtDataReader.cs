using System.Collections.Generic;
using System.Collections;
using System.Data;

using Microsoft.Data.SqlClient;

using Pansynchro.Core.Incremental;
using System.Linq;
using System;

namespace Pansynchro.Connectors.MSSQL.Incremental
{
    internal class MssqlCtDataReader : IncrementalDataReader
    {
        public MssqlCtDataReader(SqlDataReader reader, int bookmarkLength) : base(reader, bookmarkLength)
        { }

        private int[]? _columns;

        public override IEnumerable<int> AffectedColumns
        {
            get
            {
                _columns ??= Enumerable.Range(0, _reader.FieldCount - HiddenColumns).ToArray();
                return _columns;
            }
        }

        protected override int HiddenColumns => 2;

        public override UpdateRowType UpdateType => _reader.GetChar(1) switch
        {
            'D' => UpdateRowType.Delete,
            'I' => UpdateRowType.Insert,
            'U' => UpdateRowType.Update,
            _ => throw new DataException($"Invalid update type {_reader.GetChar(1)}.")
        };

        private long? _lastBookmark;

        protected override void PrepareBookmark() 
            => _lastBookmark = _reader.IsDBNull(0) ? null : _reader.GetInt64(0);

        protected override string? SaveBookmark() => _lastBookmark?.ToString();
    }
}