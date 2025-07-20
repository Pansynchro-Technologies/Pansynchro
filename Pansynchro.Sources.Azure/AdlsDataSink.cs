using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using DotNet.Globbing;

using Pansynchro.Core;

namespace Pansynchro.Sources.Azure
{
	record AzureWriteConfig(AzureConnection Conn, string Bucket, AzurePattern[] Files, string? MissingFilenameSpec)
		: AzureConfig(Conn, Bucket, Files);

	public class AdlsDataSink : IDataSink
	{
		private readonly AzureWriteConfig _config;

		public AdlsDataSink(string config)
		{
			_config = JsonSerializer.Deserialize<AzureWriteConfig>(config)
				?? throw new ArgumentException("Invalid ADLS Data Sink configuration.");
		}

		private DataLakeFileSystemClient? _fileSystemClient;

		private DataLakeFileSystemClient Client
		{
			get {
				_fileSystemClient ??= AdlsDataSource.GetDataLakeServiceClientSAS(_config.Conn.AccountName, _config.Conn.SasToken, _config.Bucket);
				return _fileSystemClient;
			}
		}

		public async Task<Stream> WriteData(string streamName)
		{
			var match = _config.Files
				.FirstOrDefault(p => Glob.Parse(p.Pattern).IsMatch(streamName))
				?.StreamName
				??_config.MissingFilenameSpec;
			if (string.IsNullOrWhiteSpace(match)) {
				throw new ArgumentException($"No ADLS file name could be generated for stream name '{streamName}'");
			}
			var filename = match.Replace("*", streamName);
			return await (await Client.CreateFileAsync(filename)).Value.OpenWriteAsync(true);
		}
	}
}
