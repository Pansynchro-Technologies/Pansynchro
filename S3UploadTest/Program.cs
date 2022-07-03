using System.Collections.Generic;
using System.Threading.Tasks;
using Pansynchro.Connectors.Parquet;
using Pansynchro.Connectors.TextFile.WholeFile;
using Pansynchro.Core;
using Pansynchro.Sources.Files;
using Pansynchro.Sources.S3;

namespace S3UploadTest
{
    public class Program
    {
        private const string FILES = @"{
  ""Files"": [
    {
      ""Name"": ""userdata"",
      ""File"": [""E:\\Parquet\\userdata1_out.parquet""]
    }
  ]
}";

        private const string OUT_FILES = @"{
  ""Files"": [
    {
      ""StreamName"": ""userdata"",
      ""FileName"": ""E:\\Parquet\\userdata1_out2.parquet""
    }
  ]
}";


        public static async Task Main()
        {
            var factory = new FileDataSourceFactory();
            var source = factory.GetSource(FILES);
            var readerFactory = new ParquetConnector();
            var reader = readerFactory.GetReader("");
            ((ISourcedConnector)reader).SetDataSource(source);
            var writer = readerFactory.GetWriter("");
            var sink = factory.GetSink(OUT_FILES);
            ((ISinkConnector)writer).SetDataSink(sink);
            var analyzer = readerFactory.GetAnalyzer("");
            ((ISourcedConnector)analyzer).SetDataSource(source);
            var dict = await analyzer.AnalyzeAsync("Userdata");

            var input = reader.ReadFrom(dict);
            await writer.Sync(input, dict);
        }
    }

    class Renamer : ITransformer
    {
        private const string PREFIX = "C:\\Users\\mason\\source\\repos\\Pansynchro-Technologies\\Pansynchro\\";

        public async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
        {
            await foreach (var (name, settings, reader) in input) {
                var outName = name.ToString().Substring(PREFIX.Length).Replace('\\', '/');
                yield return new DataStream(new(null, outName), settings, reader);
            }
        }
    }
}