using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Pansynchro.PanSQL.Core
{
	public static class CompressionHelper
	{
		public static string ToCompressedString(this string value)
		{
			using var output = new MemoryStream();
			using var encoder = new BrotliStream(output, CompressionLevel.SmallestSize);
			using var writer = new BinaryWriter(encoder, Encoding.UTF8);
			writer.Write(value);
			writer.Flush();
			return Convert.ToBase64String(output.ToArray());
		}

		public static string Decompress(string value)
		{
			using var input = new MemoryStream(Convert.FromBase64String(value));
			using var decoder = new BrotliStream(input, CompressionMode.Decompress);
			using var output = new MemoryStream();
			decoder.CopyTo(output);
			output.Position = 0;
			using var reader = new BinaryReader(output, Encoding.UTF8);
			return reader.ReadString();
		}
	}
}
