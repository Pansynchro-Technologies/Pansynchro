using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Microsoft.Data.SqlClient;

using Pansynchro.Core.Incremental;

namespace Pansynchro.Connectors.MSSQL
{
    internal class MssqlCdcDataReader : IncrementalDataReader
    {
        private readonly int[] _fieldMap;

        public MssqlCdcDataReader(SqlDataReader reader, int bookmarkLength, string[] fieldMap) : base(reader, bookmarkLength)
        {
            _fieldMap = Harmonize(fieldMap, reader);
        }

        private int[] Harmonize(string[] fieldMap, SqlDataReader reader)
        {
            var columnNames = reader.GetColumnSchema().Select(c => c.ColumnName).ToArray();
            return fieldMap.Select(f => Array.IndexOf(columnNames, f) - HiddenColumns)
                .ToArray();
        }

        public override UpdateRowType UpdateType => _reader.GetInt32(1) switch {
            1 => UpdateRowType.Delete,
            2 => UpdateRowType.Insert,
            4 => UpdateRowType.Update,
            _ => throw new DataException($"Invalid update type {_reader.GetInt32(1)}.")
        };

        public override IEnumerable<int> AffectedColumns
        {
            get {
                var mask = (byte[])_reader.GetValue(2);
                Array.Reverse(mask);
                var bits = new BitArray(mask);
                for (int i = 0; i < bits.Length; ++i) {
                    if (bits[i]) {
                        yield return _fieldMap[i];
                    }
                }
            }
        }

        protected override int HiddenColumns => 3;

        private byte[]? _lastBookmark;

        protected override void PrepareBookmark()
        {
            _lastBookmark = (byte[])_reader.GetValue(0);
        }

        protected override string? SaveBookmark()
        {
            return _lastBookmark == null ? null : BitConverter.ToString(_lastBookmark).Replace("-", "");
        }
    }
}