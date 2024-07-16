using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;

using Newtonsoft.Json.Linq;

using Pansynchro.Core;

namespace Pansynchro.Sources.Http
{
	public class HttpDataSource : IDataSource
	{
		protected readonly Dictionary<string, string[]> _urls;

		public HttpDataSource(string connectionString)
		{
			var urls = JObject.Parse(connectionString)["Urls"] as JObject;
			if (urls == null) {
				throw new ArgumentException("Connection string is missing its Urls property");
			}
			var data = urls.ToObject<Dictionary<string, string[]>>();
			if (data == null) {
				throw new ArgumentException("Connection string Urls property is in the wrong format");
			}
			_urls = data;
		}

		protected IEnumerable<(string name, string url)> GetUrls()
		{
			foreach (var (name, list) in _urls) {
				foreach (var url in list) {
					yield return (name, url);
				}
			}
		}

		public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
		{
			using var client = new HttpClient();
			foreach (var (name, url) in GetUrls()) {
				yield return (name, await client.GetStreamAsync(url));
			}
		}

		public async IAsyncEnumerable<Stream> GetDataAsync(string name)
		{
			using var client = new HttpClient();
			foreach (var (lName, url) in GetUrls()) {
				if (lName == name) {
					yield return await client.GetStreamAsync(url);
				}
			}
		}

		public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
		{
			using var client = new HttpClient();
			foreach (var (name, url) in GetUrls()) {
				yield return (name, new StringReader(await client.GetStringAsync(url)));
			}
		}

		public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
		{
			using var client = new HttpClient();
			foreach (var (lName, url) in GetUrls()) {
				if (lName == name) {
					yield return new StringReader(await client.GetStringAsync(url));
				}
			}
		}
	}
}
