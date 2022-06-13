using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Newtonsoft.Json;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Http
{
    public class HttpDataSource : IDataSource
    {
        private readonly string _conn;

        public HttpDataSource(string connectionString)
        {
            _conn = connectionString;
        }

        protected IEnumerable<(string name, string url)> GetUrls()
        {
            var entries = JsonConvert.DeserializeObject<Dictionary<string, string[]>>(_conn);
            foreach (var (name, list) in entries!) {
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

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource("Http", cs => new HttpDataSource(cs));
        }
    }
}
