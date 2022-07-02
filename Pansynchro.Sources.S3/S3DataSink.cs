using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using DotNet.Globbing;
using Newtonsoft.Json;

using Pansynchro.Core;
using Pansynchro.S3StreamUpload;

namespace Pansynchro.Sources.S3
{
    record S3WriteConfig(S3Connection Conn, string Bucket, S3Pattern[] Files, string? MissingFilenameSpec, int UploadPartSize = 5)
        : S3Config(Conn, Bucket, Files);

    public class S3DataSink : IDataSink, IDisposable
    {
        private readonly S3WriteConfig _config;

        private IAmazonS3? _client;

        private IAmazonS3 Client { 
            get {
                _client ??= new AmazonS3Client(
                    _config.Conn.AccessKeyId,
                    _config.Conn.SecretAccessKey,
                    RegionEndpoint.GetBySystemName(_config.Conn.RegionCode));
                return _client;
            } 
        }

        public S3DataSink(string config)
        {
            _config = JsonConvert.DeserializeObject<S3WriteConfig>(config)
                ?? throw new ArgumentException("Invalid S3 Data Sink configuration.");
        }

        private StreamTransferManager? _lastStream;

        public async Task<Stream> WriteData(string streamName)
        {
            if (_lastStream != null) {
                await _lastStream.Complete();
            }
            var match = _config.Files
                .FirstOrDefault(p => Glob.Parse(p.Pattern).IsMatch(streamName))
                ?.StreamName
                ??_config.MissingFilenameSpec;
            if (string.IsNullOrWhiteSpace(match)) {
                throw new ArgumentException($"No S3 key name could be generated for stream name '{streamName}'");
            }
            var filename = match.Replace("*", streamName);
            _lastStream = new StreamTransferManager(_config.Bucket, filename, Client)
                .PartSize(_config.UploadPartSize);
            return (await _lastStream.GetMultiPartOutputStreams())[0];
        }

        public void Dispose()
        {
            if (_lastStream != null) {
                _lastStream.Complete().GetAwaiter().GetResult();
            }
            _client?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
