using System.IO;

namespace Pansynchro.Core.Helpers
{
    public static class StreamHelper
    {
        public static Stream SeekableStream(Stream input)
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
