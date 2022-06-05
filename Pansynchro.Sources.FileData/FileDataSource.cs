using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Newtonsoft.Json;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Files
{
    public class FileDataSource : IDataSource
    {
        private readonly string _conn;

        public FileDataSource(string connectionString)
        {
            _conn = connectionString;
        }

        private IEnumerable<(string name, string filename)> GetFilenames()
        {
            var entries = JsonConvert.DeserializeObject<Dictionary<string, string[]>>(_conn);
            foreach (var (name, specs) in entries!) {
                foreach (var spec in specs) {
                    foreach (var filename in new FileSet(spec).Files) {
                        yield return (name, filename);
                    }
                }
            }
        }

        public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
        {
            foreach (var (name, filename) in GetFilenames())
            {
                yield return (name, File.OpenRead(filename));
            }
            await Task.CompletedTask; //just here to shut the compiler up
        }

        public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
        {
            foreach (var (name, filename) in GetFilenames())
            {
                yield return (name, File.OpenText(filename));
            }
            await Task.CompletedTask; //just here to shut the compiler up
        }

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource("Files", cs => new FileDataSource(cs));
        }
    }
}
