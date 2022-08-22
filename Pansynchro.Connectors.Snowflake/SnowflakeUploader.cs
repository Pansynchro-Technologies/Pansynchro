using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

using Tortuga.Data.Snowflake;

using Pansynchro.Core;
using Pansynchro.Core.Helpers;
using Pansynchro.Sources.Files;
using Pansynchro.SQL;
using System.IO.Compression;
using Pansynchro.Core.Streams;

namespace Pansynchro.Connectors.Snowflake
{
    internal class SnowflakeUploader : IDataOutputProcessor
    {
        private readonly string _connectionString;
        private readonly Action<Task> _report;
        private IDataSink? _sink;
        private KeyValuePair<string, MeteredStream> _pair;

        public SnowflakeUploader(string connectionString, Action<Task> report)
        {
            this._connectionString = connectionString;
            this._report = report;
        }

        public void SetDataSink(IDataSink sink)
        {
            _sink = sink;
        }

        public Task<Stream> WriteData(string streamName)
        {
            if (_pair.Key != streamName) {
                throw new DataException("Mismatched stream name");
            }
            return Task.FromResult<Stream>(_pair.Value);
        }

        private int _count;
        private StreamDescription? _last;

        internal async Task<MeteredStream> GetMeter(StreamDescription name)
        {
            if (_sink == null) {
                throw new DataException("SetDataSink should be called before calling GetMeter.");
            }
            if (name != _last) {
                _count = 0;
                _last = name;
            }
            ++_count;
            var streamName = name.ToString();
            var stream = (FileStream)(await _sink.WriteData(streamName + _count.ToString()));
            var cont = stream.ContinueWith(s => Upload(s, name));
            var result = new MeteredStream(cont);
            _pair = KeyValuePair.Create(streamName, result);
            return result;
        }

        private bool Upload(FileStream stream, StreamDescription streamName)
        {
            var filename = stream.Name;
            stream.Dispose();
            _report(Task.Run(async () => await UploadAsync(filename, streamName)));
            return false;
        }

        private async Task UploadAsync(string filename, StreamDescription streamName)
        {
            try {
                using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
                var stageName = GetStageName(streamName);
                await conn.OpenAsync();
                conn.Execute($"PUT file://{filename} @{stageName} auto_compress=false overwrite=true;");
                var lFilename = Path.GetFileName(filename);
                conn.Execute($"copy into {SnowflakeUploader.GetTargetName(streamName)} from '@{stageName}/{lFilename}' file_format = (type = avro) match_by_column_name = case_insensitive;");
                await conn.CloseAsync();
            } catch (Exception e) {
                Console.WriteLine(e);
                throw;
            } finally { 
                File.Delete(filename);
            }
        }

        private static string GetStageName(StreamDescription desc)
        {
            if (string.IsNullOrEmpty(desc.Namespace)) {
                return $"%\"{desc.Name}\"";
            }
            return $"\"{desc.Namespace}\".%\"{desc.Name}\"";
        }

        private static string GetTargetName(StreamDescription desc)
        {
            if (string.IsNullOrEmpty(desc.Namespace)) {
                return $"\"{desc.Name}\"";
            }
            return $"\"{desc.Namespace}\".\"{desc.Name}\"";
        }

        public void Dispose()
        {
            (_sink as IDisposable)?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}