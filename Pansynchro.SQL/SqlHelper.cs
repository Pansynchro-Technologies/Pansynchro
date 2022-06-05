using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Pansynchro.SQL
{
    public static class SqlHelper
    {
        public static IAsyncEnumerable<string> ReadStringsAsync(DbConnection conn, string query)
        {
            return ReadValuesAsync(conn, query, r => r.GetString(0));
        }

        public static async IAsyncEnumerable<T> ReadValuesAsync<T>(DbConnection conn, string sql, Func<IDataReader, T> selector, params KeyValuePair<string, object>[] values)
        {
            using var query = conn.CreateCommand();
            query.CommandText = sql;
            foreach (var pair in values)
            {
                var param = query.CreateParameter();
                param.ParameterName = pair.Key;
                param.Value = pair.Value;
                query.Parameters.Add(param);
            }
            using var reader = await query.ExecuteReaderAsync();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static IEnumerable<string> ReadStrings(DbConnection conn, string query)
        {
            return ReadValues(conn, query, r => r.GetString(0));
        }

        public static IEnumerable<T> ReadValues<T>(DbConnection conn, string sql, Func<IDataReader, T> selector)
        {
            using var query = conn.CreateCommand();
            query.CommandText = sql;
            using var reader = query.ExecuteReader();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static IEnumerable<string> ReadStrings(DbConnection conn, string query, params KeyValuePair<string, object>[] values)
        {
            return ReadValues(conn, query, r => r.GetString(0), values);
        }

        public static IEnumerable<T> ReadValues<T>(DbConnection conn, string sql, Func<IDataReader, T> selector, params KeyValuePair<string, object>[] values)
        {
            using var query = conn.CreateCommand();
            query.CommandText = sql;
            foreach (var pair in values)
            {
                var param = query.CreateParameter();
                param.ParameterName = pair.Key;
                param.Value = pair.Value;
                query.Parameters.Add(param);
            }
            using var reader = query.ExecuteReader();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static T? ReadValue<T>(DbConnection conn, string sql, params KeyValuePair<string, object>[] values)
        {
            using var query = conn.CreateCommand();
            query.CommandText = sql;
            foreach (var pair in values) {
                var param = query.CreateParameter();
                param.ParameterName = pair.Key;
                param.Value = pair.Value;
                query.Parameters.Add(param);
            }
            return (T?)Convert.ChangeType(query.ExecuteScalar(), typeof(T), CultureInfo.InvariantCulture);
        }

        public static void Execute(this DbConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 0;
            cmd.ExecuteNonQuery();
        }
    }
}
