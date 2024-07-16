using System.Collections.Generic;
using System.IO;
using System.Net.Http;

using Newtonsoft.Json.Linq;

using Pansynchro.Core;

namespace Pansynchro.Sources.Http
{
	public class RestSource : HttpDataSource, IDataSource
	{
		private string? _nextQuery;
		public RestSource(string conn) : base(conn)
		{
			_nextQuery = ExtractNextQueryString(conn);
		}

		private static string? ExtractNextQueryString(string conn)
		{
			var config = JObject.Parse(conn);
			var next = config["Next"] as JToken;
			return next?.ToString();
		}

		private async IAsyncEnumerable<TextReader> DoGetText(HttpClient client, string name, string url)
		{
			bool more = false;
			var page = url;
			do {
				var text = await client.GetStringAsync(page);
				yield return new StringReader(text);
				if (_nextQuery != null) {
					var nextToken = JToken.Parse(text).SelectToken(_nextQuery);
					if (nextToken?.Type == JTokenType.String) {
						page = (string)nextToken!;
						more = true;
					} else {
						more = false;
					}
				}
			} while (more);
		}

		public async new IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
		{
			using var client = new HttpClient();
			foreach (var (name, url) in GetUrls()) {
				await foreach (var reader in DoGetText(client, name, url)) {
					yield return (name, reader);
				}
			}
		}

		public new async IAsyncEnumerable<TextReader> GetTextAsync(string name)
		{
			using var client = new HttpClient();
			foreach (var (sName, url) in GetUrls()) {
				if (sName == name) {
					await foreach (var reader in DoGetText(client, name, url)) {
						yield return reader;
					}
				}
			}
		}
	}
}
