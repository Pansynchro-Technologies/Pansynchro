using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Tortuga.Data.Snowflake;

using Pansynchro.Connectors.Avro;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Sources.Files;
using Pansynchro.Sources.Compression;
using Pansynchro.Core.Transformations;

namespace Pansynchro.Connectors.Snowflake
{
    public class SnowflakeWriter : IWriter
    {
        private readonly string _conn;

        public SnowflakeWriter(string connectionString)
        {
            _conn = connectionString;
        }

        private const string FILE_SINK_CONFIG =
@"{{
  ""Files"": [
    {{
      ""StreamName"": ""*"",
      ""Filename"": ""{0}""
    }}
  ]
}}";
        private const long SNOWFLAKE_SIZE = (long)(1024 * 1024 * 1024 * 1.5); // 1.5 GB

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            var uploads = new List<Task>();
            using var subWriter = new AvroWriter();
            var tempdir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempdir);
            try {
                var path = Path.Combine(tempdir, "*.avro").Replace("\\", "\\\\");
                var sink = new FileDataSink(string.Format(FILE_SINK_CONFIG, path));
                var uploader = new SnowflakeUploader(_conn, uploads.Add);
                subWriter.SetDataSink(sink.Pipeline(uploader));
                var partitioner = new SizePartitionTransformer(uploader.GetMeter, SNOWFLAKE_SIZE);
                await subWriter.Sync(partitioner.Transform(streams), dest);
                await Task.WhenAll(uploads);
            } finally {
                Directory.Delete(tempdir, true);
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
