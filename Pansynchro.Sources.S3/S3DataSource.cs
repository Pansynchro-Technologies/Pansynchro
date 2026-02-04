using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using DotNet.Globbing;

using Pansynchro.Core;
using Pansynchro.Core.Helpers;

namespace Pansynchro.Sources.S3
{
	record S3Connection(string AccessKeyId, string SecretAccessKey, string RegionCode);

	record S3Pattern(string Pattern, string StreamName);

	record S3Key(string StreamName, string Bucket, string Filename);

	record S3Config(S3Connection Conn, string Bucket, S3Pattern[] Files);

	public class S3DataSource : IDataSource
	{
		private readonly S3Config _config;

		public S3DataSource(string config)
		{
			_config = JsonSerializer.Deserialize<S3Config>(config)
				?? throw new ArgumentException("Invalid S3DataSource configuration");
		}

		private AmazonS3Client GetClient()
		{
			var conn = _config.Conn;
			var region = RegionEndpoint.GetBySystemName(conn.RegionCode);
			return new AmazonS3Client(conn.AccessKeyId, conn.SecretAccessKey, region);
		}

		private async IAsyncEnumerable<S3Key> GetFiles(AmazonS3Client client)
		{
			await foreach (var result in DoGetFiles(client, _config.Bucket, _config.Files)) {
				yield return result;
			}
		}

		private async IAsyncEnumerable<S3Key> GetFiles(AmazonS3Client client, string streamName)
		{
			var files = _config.Files.Where(p => p.StreamName == streamName);
			await foreach (var result in DoGetFiles(client, _config.Bucket, files)) {
				yield return result;
			}
		}

		private static async IAsyncEnumerable<S3Key> DoGetFiles(
			AmazonS3Client client, string bucket, IEnumerable<S3Pattern> files)
		{
			var patterns = files
				.Select(g => KeyValuePair.Create(g.StreamName, Glob.Parse(g.Pattern)))
				.ToArray();
			await foreach (var filename in ListFiles(client, bucket, patterns)) {
				var match = patterns.FirstOrDefault(p => p.Value.IsMatch(filename)).Key;
				if (match != null) {
					yield return new S3Key(match, bucket, filename);
				}
			}
		}

		private static async IAsyncEnumerable<string> ListFiles(
			AmazonS3Client client, string group, KeyValuePair<string, Glob>[] patterns)
		{
			if (patterns.All(p => GlobHelper.IsLiteralPattern(p.Value))) {
				foreach (var pair in patterns) {
					yield return pair.Value.ToString();
				}
			} else if (patterns.All(p => GlobHelper.StartsWithLiteralPattern(p.Value))) {
				var starts = patterns
					.Select(p => GlobHelper.ExtractLiteralPrefix(p.Value))
					.Distinct()
					.ToArray();
				await foreach (var result in ListS3Files(client, group, starts)) {
					yield return result;
				}
			} else {
				await foreach (var result in ListS3Files(client, group)) {
					yield return result;
				}
			}
		}

		private static async IAsyncEnumerable<string> ListS3Files(AmazonS3Client client, string group, string[] starts)
		{
			foreach (var start in starts) {
				var req = new ListObjectsV2Request { BucketName = group, Prefix = start };
				await foreach (var result in DoListS3Files(client, req)) {
					yield return result;
				}
			}
		}

		private static async IAsyncEnumerable<string> ListS3Files(AmazonS3Client client, string group)
		{
			var req = new ListObjectsV2Request { BucketName = group };
			await foreach (var result in DoListS3Files(client, req)) {
				yield return result;
			}
		}

		private static async IAsyncEnumerable<string> DoListS3Files(
			AmazonS3Client client, ListObjectsV2Request req)
		{
			ListObjectsV2Response response;
			do {
				response = await client.ListObjectsV2Async(req);
				foreach (var obj in response.S3Objects) {
					if (obj.Size > 0 && !obj.Key.EndsWith('/') && !obj.Key.EndsWith('\\')) {
						yield return obj.Key;
					}
				}
				req.ContinuationToken = response.NextContinuationToken;
			}
			while (response.IsTruncated == true);
		}

		public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
		{
			using var client = GetClient();
			using var transfer = new TransferUtility(client);
			await foreach (var group in GetFiles(client).GroupBy(k => k.StreamName)) {
				foreach (var key in group) {
					yield return (group.Key, await transfer.OpenStreamAsync(key.Bucket, key.Filename));
				}
			}
		}

		public async IAsyncEnumerable<Stream> GetDataAsync(string name)
		{
			using var client = GetClient();
			using var transfer = new TransferUtility(client);
			await foreach (var key in GetFiles(client, name)) {
				yield return await transfer.OpenStreamAsync(key.Bucket, key.Filename);
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
