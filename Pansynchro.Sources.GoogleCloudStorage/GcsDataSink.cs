using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;

using DotNet.Globbing;
using Google.Cloud.Storage.V1;
using Newtonsoft.Json;

using Pansynchro.Core;

namespace Pansynchro.Sources.GoogleCloudStorage
{
    public class GcsDataSink : IDataSink, IDisposable
    {
        private readonly List<Task> _uploaders = new();
        private readonly GcsWriteConfig _config;

        public GcsDataSink(string config)
        {
            _config = JsonConvert.DeserializeObject<GcsWriteConfig>(config)
                ?? throw new ArgumentException("Invalid GcsDataSink configuration");
        }

        public async Task<Stream> WriteData(string streamName)
        {
            await TrimUploaders();
            var pipe = new Pipe();
            var reader = pipe.Reader.AsStream();
            var match = _config.Files
                .FirstOrDefault(p => Glob.Parse(p.Pattern).IsMatch(streamName))
                ?.StreamName
                ??_config.MissingFilenameSpec;
            if (string.IsNullOrWhiteSpace(match)) {
                throw new ArgumentException($"No GCS key name could be generated for stream name '{streamName}'");
            }
            var filename = match.Replace("*", streamName);
            var uploader = Task.Run(async () => {
                using var client = await StorageClient.CreateAsync();
                await client.UploadObjectAsync(_config.Bucket, filename, null, reader);
            });
            _uploaders.Add(uploader);
            return pipe.Writer.AsStream();
        }

        private async ValueTask TrimUploaders()
        {
            _uploaders.RemoveAll(t => t.IsCompleted || t.IsCanceled);
            var errored = _uploaders.Where(t => t.IsFaulted).ToArray();
            if (errored.Length > 0) {
                _uploaders.RemoveAll(t => t.IsFaulted);
                await Task.WhenAll(errored); //throw relevant error(s)
            }
            while (_uploaders.Count > _config.MaxParallelism) {
                var done = await Task.WhenAny(_uploaders);
                _uploaders.Remove(done);
            }
        }

        public void Dispose()
        {
            Task.WhenAll(_uploaders).GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }
    }
}