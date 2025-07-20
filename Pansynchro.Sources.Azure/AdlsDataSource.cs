using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using DotNet.Globbing;

using Pansynchro.Core;
using Pansynchro.Core.Helpers;

namespace Pansynchro.Sources.Azure
{
	internal record AzurePattern(string Pattern, string StreamName);
	internal record AzureConnection(string AccountName, string SasToken);
	internal record AzureConfig(AzureConnection Conn, string FileSystem, AzurePattern[] Files);

	public class AdlsDataSource : IDataSource
	{
		private AzureConfig _config = null!;

		public AdlsDataSource(string config)
		{
			_config = JsonSerializer.Deserialize<AzureConfig>(config)
				?? throw new ArgumentException("Invalid ADLS Data Source configuration");
		}

		public static DataLakeFileSystemClient GetDataLakeServiceClientSAS(string accountName, string sasToken, string fileSystemName)
		{
			string dfsUri = $"https://{accountName}.dfs.core.windows.net";

			var dataLakeServiceClient = new DataLakeServiceClient(
				new Uri(dfsUri),
				new AzureSasCredential(sasToken));

			return dataLakeServiceClient.GetFileSystemClient(fileSystemName);
		}

		private async IAsyncEnumerable<DataLakeFileClient> GetFiles(DataLakeFileSystemClient client)
		{
			await foreach (var result in DoGetFiles(client, _config.Files)) {
				yield return result;
			}
		}

		private async IAsyncEnumerable<DataLakeFileClient> GetFiles(DataLakeFileSystemClient client, string streamName)
		{
			var files = _config.Files.Where(p => p.StreamName == streamName);
			await foreach (var result in DoGetFiles(client, files)) {
				yield return result;
			}
		}

		private static async IAsyncEnumerable<DataLakeFileClient> DoGetFiles(
			DataLakeFileSystemClient client, IEnumerable<AzurePattern> files)
		{
			var patterns = files
				.Select(g => KeyValuePair.Create(g.StreamName, Glob.Parse(g.Pattern)))
				.ToArray();
			await foreach (var fileClient in ListFiles(client, patterns)) {
				var match = patterns.FirstOrDefault(p => p.Value.IsMatch(fileClient.Name)).Key;
				if (match != null) {
					yield return fileClient;
				}
			}
		}

		private static async IAsyncEnumerable<DataLakeFileClient> ListFiles(
			DataLakeFileSystemClient client, KeyValuePair<string, Glob>[] patterns)
		{
			if (patterns.All(p => GlobHelper.IsLiteralPattern(p.Value))) {
				foreach (var pair in patterns) {
					yield return client.GetFileClient(pair.Value.ToString());
				}
			} else if (patterns.All(p => GlobHelper.StartsWithLiteralPattern(p.Value))) {
				var starts = patterns
					.Select(p => GlobHelper.ExtractLiteralPrefix(p.Value))
					.Distinct()
					.ToArray();
				await foreach (var result in ListAzureFiles(client, starts)) {
					yield return result;
				}
			} else {
				await foreach (var result in ListAzureFiles(client)) {
					yield return result;
				}
			}
		}

		private static async IAsyncEnumerable<DataLakeFileClient> ListAzureFiles(DataLakeFileSystemClient client, string[] starts)
		{
			foreach (var start in starts) {
				var dir = client.GetDirectoryClient(start);
				await foreach (var result in DoListAzureFiles(client, dir.GetPathsAsync(recursive: true))) {
					yield return result;
				}
			}
		}

		private static async IAsyncEnumerable<DataLakeFileClient> ListAzureFiles(DataLakeFileSystemClient client)
		{
			await foreach (var result in DoListAzureFiles(client, client.GetPathsAsync(recursive: true))) {
				yield return result;
			}
		}

		private static async IAsyncEnumerable<DataLakeFileClient> DoListAzureFiles(DataLakeFileSystemClient client, AsyncPageable<PathItem> paths)
		{
			await foreach (var path in paths) {
				if (path.IsDirectory != true && path.ContentLength > 0) {
					yield return client.GetFileClient(path.Name);
				}
			}
		}

		public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
		{
			var client = GetDataLakeServiceClientSAS(_config.Conn.AccountName, _config.Conn.SasToken, _config.FileSystem);
			await foreach (var file in GetFiles(client)) {
				yield return (file.Name, await file.OpenReadAsync());
			}
		}

		public async IAsyncEnumerable<Stream> GetDataAsync(string name)
		{
			var client = GetDataLakeServiceClientSAS(_config.Conn.AccountName, _config.Conn.SasToken, _config.FileSystem);
			await foreach (var file in GetFiles(client, name)) {
				yield return await file.OpenReadAsync();
			}
		}

		public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
		{
			await foreach (var (name, stream) in GetDataAsync()) {
				yield return (name, new StreamReader(stream));
			}
		}

		public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
		{
			await foreach (var stream in GetDataAsync(name)) {
				yield return new StreamReader(stream);
			}
		}
	}
}
