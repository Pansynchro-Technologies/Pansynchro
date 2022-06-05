using System;
using System.Collections.Generic;

using Npgsql;

namespace Pansynchro.Connectors.Postgres
{
    public static class PostgresHelper
    {
        public static IAsyncEnumerable<string> ReadStringsAsync(NpgsqlConnection conn, string query)
        {
            return ReadValuesAsync(conn, query, r => r.GetString(0));
        }

        public static async IAsyncEnumerable<T> ReadValuesAsync<T>(NpgsqlConnection conn, string sql, Func<NpgsqlDataReader, T> selector)
        {
            using var query = new NpgsqlCommand(sql, conn);
            using var reader = await query.ExecuteReaderAsync();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static IEnumerable<string> ReadStrings(NpgsqlConnection conn, string query)
        {
            return ReadValues(conn, query, r => r.GetString(0));
        }

        public static IEnumerable<T> ReadValues<T>(NpgsqlConnection conn, string sql, Func<NpgsqlDataReader, T> selector)
        {
            using var query = new NpgsqlCommand(sql, conn);
            using var reader = query.ExecuteReader();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static IEnumerable<string> ReadStrings(NpgsqlConnection conn, string query, params KeyValuePair<string, object>[] values)
        {
            return ReadValues(conn, query, r => r.GetString(0), values);
        }

        public static IEnumerable<T> ReadValues<T>(NpgsqlConnection conn, string sql, Func<NpgsqlDataReader, T> selector, params KeyValuePair<string, object>[] values)
        {
            using var query = new NpgsqlCommand(sql, conn);
            foreach (var pair in values)
            {
                query.Parameters.AddWithValue(pair.Key, pair.Value);
            }
            using var reader = query.ExecuteReader();
            while (reader.Read())
            {
                yield return selector(reader);
            }
        }

        public static void Execute(this NpgsqlConnection conn, string sql)
        {
            using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = 0 };
            cmd.ExecuteNonQuery();
        }
    }
}
