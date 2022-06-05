using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Pansynchro.Core;

namespace Pansynchro.Sources.Http
{
    public class RestSource : HttpDataSource, IDataSource
    {
        private string? _nextQuery;
        public RestSource(string conn) : base(ExtractBaseConnectionString(conn))
        {
            _nextQuery = ExtractNextQueryString(conn);
        }

        private static string ExtractBaseConnectionString(string conn)
        {
            var config = JObject.Parse(conn);
            if (config == null) {
                throw new ArgumentException("REST Source configuration string is missing");
            }
            var urls = config["Urls"] as JObject;
            if (urls == null) {
                throw new ArgumentException("REST Source configuration string has no 'Urls' section.");
            }
            return urls.ToString();
        }

        private static string? ExtractNextQueryString(string conn)
        {
            var config = JObject.Parse(conn);
            var next = config["Next"] as JToken;
            return next?.ToString();
        }

        public async new IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
        {
            using var client = new HttpClient();
            foreach (var (name, url) in GetUrls()) {
                bool more = false;
                var page = url;
                do {
                    var text = await client.GetStringAsync(page);
                    yield return (name, new StringReader(text));
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
        }
    }
}
