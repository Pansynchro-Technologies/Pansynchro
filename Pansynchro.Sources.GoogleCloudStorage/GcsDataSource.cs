using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;

using DotNet.Globbing;
using Google.Cloud.Storage.V1;
using GObject = Google.Apis.Storage.v1.Data.Object;
using Newtonsoft.Json;

using Pansynchro.Core;
using Pansynchro.Core.Helpers;

namespace Pansynchro.Sources.GoogleCloudStorage
{
	public class GcsDataSource : IDataSource, IDisposable
	{
		private readonly GcsReadConfig _config;
		private readonly List<Task> _ongoing = new();

		public GcsDataSource(string config)
		{
			_config = JsonConvert.DeserializeObject<GcsReadConfig>(config)
				?? throw new ArgumentException("Invalid GcsDataSource configuration");
		}

		public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
		{
			using var client = await CreateClient();
			await foreach (var group in GetFiles(client).GroupBy(k => k.stream)) {
				await foreach (var key in group) {
					yield return (group.Key, await DownloadFile(client, key.obj));
				}
			}
		}

		public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
		{
			await foreach (var (name, stream) in GetDataAsync()) {
				yield return (name, new StreamReader(stream));
			}
		}

		public async IAsyncEnumerable<Stream> GetDataAsync(string name)
		{
			using var client = await CreateClient();
			await foreach (var obj in GetFiles(client, name)) {
				yield return await DownloadFile(client, obj);
			}
		}

		public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
		{
			await foreach (var stream in GetDataAsync(name)) {
				yield return new StreamReader(stream);
			}
		}

		private async ValueTask<StorageClient> CreateClient()
		{
			return await StorageClient.CreateAsync();
		}

		private async ValueTask<Stream> DownloadFile(StorageClient client, GObject key)
		{
			await TrimOngoing();
			var pipe = new Pipe();
			var writer = pipe.Writer;
			_ongoing.Add(Task.Run(() => client.DownloadObject(key, writer.AsStream())));
			return pipe.Reader.AsStream();
		}

		private async ValueTask TrimOngoing()
		{
			_ongoing.RemoveAll(t => t.IsCompleted || t.IsCanceled);
			var errored = _ongoing.Where(t => t.IsFaulted).ToArray();
			if (errored.Length > 0) {
				_ongoing.RemoveAll(t => t.IsFaulted);
				await Task.WhenAll(errored); //throw relevant error(s)
			}
			while (_ongoing.Count > _config.MaxParallelism) {
				var done = await Task.WhenAny(_ongoing);
				_ongoing.Remove(done);
			}
		}

		private async IAsyncEnumerable<(string stream, GObject obj)> GetFiles(StorageClient client)
		{
			await foreach (var result in DoGetFiles(client, _config.Bucket, _config.Streams)) {
				yield return result;
			}
		}

		private async IAsyncEnumerable<GObject> GetFiles(StorageClient client, string streamName)
		{
			var files = _config.Streams.Where(p => p.StreamName == streamName);
			await foreach (var result in DoGetFiles(client, _config.Bucket, files)) {
				yield return result.obj;
			}
		}

		private static async IAsyncEnumerable<(string stream, GObject obj)> DoGetFiles(
			StorageClient client, string bucket, IEnumerable<GcsPattern> files)
		{
			var patterns = files
				.Select(g => KeyValuePair.Create(g.StreamName, Glob.Parse(g.Pattern)))
				.ToArray();
			await foreach (var obj in ListFiles(client, bucket, patterns)) {
				var match = patterns.FirstOrDefault(p => p.Value.IsMatch(obj.Name)).Key;
				if (match != null) {
					yield return (match, obj);
				}
			}
		}

		private static async IAsyncEnumerable<GObject> ListFiles(
			StorageClient client, string group, KeyValuePair<string, Glob>[] patterns)
		{
			if (patterns.All(p => GlobHelper.StartsWithLiteralPattern(p.Value))) {
				var starts = patterns
					.Select(p => GlobHelper.ExtractLiteralPrefix(p.Value))
					.Distinct()
					.ToArray();
				await foreach (var result in DoListFiles(client, group, starts)) {
					yield return result;
				}
			} else {
				await foreach (var result in client.ListObjectsAsync(group)) {
					yield return result;
				}
			}
		}

		private static async IAsyncEnumerable<GObject> DoListFiles(StorageClient client, string group, string[] starts)
		{
			foreach (var start in starts) {
				await foreach (var result in client.ListObjectsAsync(group, start)) {
					yield return result;
				}
			}
		}

		public void Dispose()
		{
			Task.WhenAll(_ongoing).GetAwaiter().GetResult();
			GC.SuppressFinalize(this);
		}
	}
}
