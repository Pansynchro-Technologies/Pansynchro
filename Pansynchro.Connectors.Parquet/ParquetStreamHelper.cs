using System.IO;

namespace Pansynchro.Connectors.Parquet
{
    internal static class ParquetStreamHelper
    {
        internal static Stream CheckStream(Stream input)
        {
            if (input.CanSeek) {
                return input;
            }
            var result = new MemoryStream();
            input.CopyTo(result);
            return result;
        }
    }
}
