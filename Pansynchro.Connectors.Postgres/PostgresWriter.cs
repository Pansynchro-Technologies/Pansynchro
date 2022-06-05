using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using Npgsql;
using NpgsqlTypes;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Postgres
{
    public class PostgresWriter : IWriter
    {
        private readonly NpgsqlConnection _conn;
        private DataDictionary? _dict;

        public PostgresWriter(string connectionString)
        {
            _conn = new NpgsqlConnection(connectionString);
        }

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            Setup(dest);
            await _conn.OpenAsync();
            try {
                await foreach (var (name, _, reader) in streams) {
                    try {
                        BinCopy(name, reader);
                    } finally {
                        reader.Dispose();
                    }
                }
                Finish();
            } finally {
                await _conn.CloseAsync();
            }
        }

        private void BinCopy(StreamDescription name, IDataReader reader)
        {
            Console.WriteLine($"{DateTime.Now}: Writing stream '{name}'.");
            var schema = ExtractSchema(name);
            var fields = string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(i => '"' + reader.GetName(i).ToLower(CultureInfo.InvariantCulture) + '"'));
            using var importer = _conn.BeginBinaryImport($"COPY Pansynchro.\"{name.Name.ToLower(CultureInfo.InvariantCulture)}\" ({fields}) FROM STDIN (FORMAT BINARY)");
            var loader = BuildLoader(reader, schema);
            var buffer = new object[reader.FieldCount];
            while (reader.Read())
            {
                loader(reader, importer, buffer);
            }
            importer.Complete();
        }

        private void Finish()
        {
            foreach (var table in _dict!.DependencyOrder.SelectMany(sd => sd).Reverse()) {
                Console.WriteLine($"{DateTime.Now}: Merging table '{table}'");
                MetadataHelper.MergeTable(_conn, table.Name, table.Namespace!);
            }
            Console.WriteLine($"{DateTime.Now}: Truncating");
            foreach (var table in _dict.DependencyOrder.SelectMany(sd => sd)) {
                MetadataHelper.TruncateTable(_conn, table.Name);
            }
        }

        private static Action<IDataReader, NpgsqlBinaryImporter, object[]> BuildLoader(IDataReader reader, Dictionary<string, NpgsqlDbType?> schema)
        {
            Action<NpgsqlBinaryImporter, object[]> result = null!;
            for (int i = 0; i < reader.FieldCount; ++i) {
                var fieldName = reader.GetName(i);
                var dataType = schema[fieldName];
                result += MakeReader(i, dataType!.Value);
            }
            return (r, imp, buffer) => {
                reader.GetValues(buffer);
                imp.StartRow();
                result(imp, buffer);
            };
        }

        private static Action<NpgsqlBinaryImporter, object[]> MakeReader(int i, NpgsqlDbType value)
        {
            return (imp, buffer) => imp.Write(buffer[i], value);
        }

        private Dictionary<string, NpgsqlDbType?> ExtractSchema(StreamDescription name)
        {
            using var cmd = new NpgsqlCommand($"select * from Pansynchro.{name.Name} where 1 = 0", _conn);
            using var reader = cmd.ExecuteReader();
            var schema = reader.GetColumnSchema();
            return schema.ToDictionary(c => c.ColumnName, c => c.NpgsqlDbType, StringComparer.InvariantCultureIgnoreCase);
        }

        private void Setup(DataDictionary dest)
        {
            _dict = dest;
            _conn.OpenAsync().GetAwaiter().GetResult();
            try
            {
                MetadataHelper.EnsureScratchTables(_conn, dest);
            } finally {
                _conn.Close();
            }
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
