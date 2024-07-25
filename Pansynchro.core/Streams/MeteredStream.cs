using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Pansynchro.Core.Streams
{
	public class MeteredStream : Stream
	{
		private readonly Stream _baseStream;

		private long _read;
		private long _written;

		public MeteredStream(Stream inner)
		{
			_baseStream = inner;
		}

		public long TotalBytesRead => _read;

		public long TotalBytesWritten => _written;

		public override void Flush() => _baseStream.Flush();

		public override Task FlushAsync(CancellationToken cancellationToken) => _baseStream.FlushAsync(cancellationToken);

		public override long Seek(long offset, SeekOrigin origin) => _baseStream.Seek(offset, origin);

		public override void SetLength(long value) => _baseStream.SetLength(value);

		public override int Read(byte[] buffer, int offset, int count)
		{
			var result = _baseStream.Read(buffer, offset, count);
			_read += result;
			return result;
		}

		public override int Read(System.Span<byte> buffer)
		{
			var result = _baseStream.Read(buffer);
			_read += result;
			return result;
		}

		public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			var result = await _baseStream.ReadAsync(buffer, offset, count, cancellationToken);
			_read += result;
			return result;
		}

		public override async ValueTask<int> ReadAsync(System.Memory<byte> buffer, CancellationToken cancellationToken = default)
		{
			var result = await _baseStream.ReadAsync(buffer, cancellationToken);
			_read += result;
			return result;
		}

		public override int ReadByte()
		{
			++_read;
			return _baseStream.ReadByte();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			_baseStream.Write(buffer, offset, count);
			_written += count;
		}

		public override void Write(System.ReadOnlySpan<byte> buffer)
		{
			_baseStream.Write(buffer);
			_written += buffer.Length;
		}

		public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			await _baseStream.WriteAsync(buffer, offset, count, cancellationToken);
			_written += count;
		}

		public override async ValueTask WriteAsync(System.ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
		{
			await _baseStream.WriteAsync(buffer, cancellationToken);
			_written += buffer.Length;
		}

		public override void WriteByte(byte value)
		{
			++_written;
			_baseStream.WriteByte(value);
		}

		public override void CopyTo(Stream destination, int bufferSize)
		{
			var start = Position;
			_baseStream.CopyTo(destination, bufferSize);
			_read += Position - start;
		}

		public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
		{
			var start = Position;
			await base.CopyToAsync(destination, bufferSize, cancellationToken);
			_read += Position - start;
		}

		public override void Close() => _baseStream.Close();

		public override bool CanRead => _baseStream.CanRead;

		public override bool CanWrite => _baseStream.CanWrite;

		public override bool CanSeek => _baseStream.CanSeek;

		public override long Length => _baseStream.Length;

		public override bool CanTimeout => _baseStream.CanTimeout;

		public override int ReadTimeout
		{
			get => _baseStream.ReadTimeout;
			set => _baseStream.ReadTimeout = value;
		}

		public override int WriteTimeout
		{
			get => _baseStream.WriteTimeout;
			set => _baseStream.WriteTimeout = value;
		}

		public override long Position
		{
			get => _baseStream.Position;
			set => _baseStream.Position = value;
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
			if (disposing) {
				_baseStream.Dispose();
			}
		}

		public override async ValueTask DisposeAsync()
		{
			await base.DisposeAsync();
			await _baseStream.DisposeAsync();
		}
	}
}
