using System;
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

		public static Stream ContinueWith<T>(this T stream, Func<T, bool> continuation) where T : Stream
			=> new ContinuationStream<T>(stream, continuation);
	}

	internal class ContinuationStream<T> : Stream where T : Stream
	{
		private readonly T _baseStream;
		private readonly Func<T, bool> _continuation;

		internal ContinuationStream(T baseStream, Func<T, bool> continuation)
		{
			_baseStream = baseStream;
			_continuation = continuation;
		}

		public override bool CanRead => _baseStream.CanRead;

		public override bool CanSeek => _baseStream.CanSeek;

		public override bool CanWrite => _baseStream.CanWrite;

		public override long Length => _baseStream.Length;

		public override long Position { get => _baseStream.Position; set => _baseStream.Position = value; }

		public override void Flush() => _baseStream.Flush();

		private readonly byte[] _oneByte = new byte[1];

		public override int Read(byte[] buffer, int offset, int count)
			=> _baseStream.Read(buffer, offset, count);

		public override int ReadByte() => Read(_oneByte, 0, 1) == 0 ? -1 : _oneByte[0];

		public override long Seek(long offset, SeekOrigin origin) => _baseStream.Seek(offset, origin);

		public override void SetLength(long value) => _baseStream.SetLength(value);

		public override void Write(byte[] buffer, int offset, int count)
			=> _baseStream.Write(buffer, offset, count);

		public override void WriteByte(byte value)
		{
			_oneByte[0] = value;
			Write(_oneByte, 0, 1);
		}

		protected override void Dispose(bool disposing)
		{
			if (_continuation(_baseStream)) {
				_baseStream.Dispose();
			}
		}
	}
}
