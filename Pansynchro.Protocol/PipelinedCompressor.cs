using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Pansynchro.Protocol
{
    internal sealed class PipelinedCompressor : Stream
    {
        private readonly BrotliEncoder _encoder;
        private readonly byte[] _outputBuffer = new byte[65520]; //size used by BrotliStream.cs
        private readonly Pipe _pipe = new();
        private readonly Stream _writeStream;
        private readonly Task _copyTask;

        private bool _active = true;
        public bool Active { 
            get => _active;
            set {
                if (value != _active) {
                    if (!value) {
                        Flush();
                    }
                    _active = value;
                }
            }
        }

        public PipelinedCompressor(int compressionLevel, Stream output)
        {
            _encoder = new BrotliEncoder(compressionLevel, 22); //default window size
            _writeStream = _pipe.Writer.AsStream();
            _copyTask = Task.Run(async () => {
                await _pipe.Reader.CopyToAsync(output);
                await output.FlushAsync();
            });
        }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new System.NotImplementedException();

        public override long Position { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }

        public override void Flush()
        {
            var output = _outputBuffer.AsSpan();
            OperationStatus lastResult = OperationStatus.DestinationTooSmall;
            while (lastResult == OperationStatus.DestinationTooSmall) {
                lastResult = _encoder.Flush(output, out var count);
                if (lastResult == OperationStatus.InvalidData) {
                    throw new InvalidDataException("Unable to compress data");
                }
                if (count > 0) {
                    _writeStream.Write(output.Slice(0, count));
                }
            }
            _writeStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new System.NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new System.NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new System.NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var bytes = new ReadOnlySpan<byte>(buffer, offset, count);
            DoWrite(bytes);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            DoWrite(buffer);
        }

#if NET7_0_OR_GREATER
        public override void WriteByte(byte value)
        {
            DoWrite(new ReadOnlySpan<byte>(in value));
        }
#else
        private readonly byte[] _oneByte = new byte[1];
        public override void WriteByte(byte value)
        {
            _oneByte[0] = value;
            DoWrite(_oneByte.AsSpan());
        }

#endif

        private void DoWrite(ReadOnlySpan<byte> bytes)
        {
            if (_active) { 
                var output = _outputBuffer.AsSpan();
                var lastResult = OperationStatus.DestinationTooSmall;
                while (lastResult == OperationStatus.DestinationTooSmall) {
                    lastResult = _encoder.Compress(bytes, output, out var consumed, out var written, false);
                    if (lastResult == OperationStatus.InvalidData) {
                        throw new InvalidOperationException("Unable to compress data");
                    }
                    if (written > 0) { 
                        _writeStream.Write(output.Slice(0, written));
                    }
                    if (consumed > 0) { 
                        bytes = bytes.Slice(consumed);
                    }
                }
            } else { 
                _writeStream.Write(bytes);
            }
        }

        public async Task FinishedAsync()
        {
            Flush();
            _pipe.Writer.Complete();
            await _copyTask.ConfigureAwait(false);
        }
    }
}
