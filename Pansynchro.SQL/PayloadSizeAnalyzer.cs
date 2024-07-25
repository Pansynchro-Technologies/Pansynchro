using System;
using System.Data.Common;
using System.Linq;

using Pansynchro.Core.DataDict;

namespace Pansynchro.SQL
{
	public static class PayloadSizeAnalyzer
	{
		public static int AverageSize(DbConnection conn, StreamDefinition table, ISqlFormatter formatter, DbTransaction tran)
		{
			using var query = conn.CreateCommand();
			query.Transaction = tran;
			var tableName = table.Name;
			var columnList = string.Join(", ", table.NameList.Select(formatter.QuoteName));
			query.CommandText = formatter.LimitRows($"select {columnList} from {formatter.QuoteName(tableName)}", 100);
			using var reader = query.ExecuteReader();
			int results = 0;
			int payload = 0;
			var buffer = new object[reader.FieldCount];
			while (reader.Read()) {
				++results;
				reader.GetValues(buffer);
				payload += buffer.Select(GetSize).Sum();
			}
			return results == 0 ? 0 : (payload / results);
		}

		private static int GetSize(object value)
		{
			var type = value.GetType();
			if (type.IsArray) {
				var arr = (Array)value;
				return arr.Cast<object>().Select(GetSize).Sum();
			}
			if (type == typeof(DBNull)) {
				return 4;
			}
			if (type == typeof(int)) {
				return 4;
			}
			if (type == typeof(long)) {
				return 8;
			}
			if (type == typeof(float)) {
				return 4;
			}
			if (type == typeof(double)) {
				return 8;
			}
			if (type == typeof(bool)) {
				return 1;
			}
			if (type == typeof(string)) {
				return ((string)value).Length;
			}
			if (type == typeof(byte)) {
				return 1;
			}
			if (type == typeof(byte[])) {
				return ((byte[])value).Length;
			}
			if (type == typeof(System.Guid)) {
				return 16;
			}
			if (type == typeof(decimal)) {
				return 16;
			}
			if (type == typeof(short)) {
				return 2;
			}
			if (type == typeof(DateTime)) {
				return 8;
			}
			if (type == typeof(TimeSpan)) {
				return 5;
			}
			// no actual type check to remove dependency on SQL Server specific assembly
			if (type.Name is "SqlHierarchyId" or "SqlGeography") {
				return value.ToString()!.Length;
			}
			throw new ArgumentException($"Unsupported data type '{type}'.");
		}
	}
}
