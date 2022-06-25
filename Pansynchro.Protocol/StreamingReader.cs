using System;
using System.Data;
using System.IO;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.Protocol
{
    /// <summary>
    ///  IDataReader implementation to decode data encoded by BinaryEncoder.
    ///  Because the data is coming from a serialized stream that contains multiple contiguous data
    ///  sources, this reader must be read to completion before retrieving the next one.
    ///  Failure to do so will throw an IOException.
    /// </summary>
    internal class StreamingReader : ArrayReader
    {
        private readonly BinaryReader _reader;
        private readonly BinaryReader _blockReader;
        private readonly Func<BinaryReader, object>[] _decoders;
        private readonly int _rcfFieldCount;
        private readonly int _seqIdField;
        private readonly bool _seqIdIsLong;
        private readonly Action _onComplete;
        private MemoryStream _bufferStream = new();

        public StreamingReader(BinaryReader reader, StreamDefinition schema, DataDictionary dict, StreamMode mode, Action onComplete)
        {
            this._reader = reader;
            this._decoders = BinaryDecoder.BuildBufferDecoders(schema, dict);
            this._onComplete = onComplete;
            if (mode != StreamMode.InsertOnly) {
                throw new InvalidDataException("Invalid stream mode.");
            }
            _buffer = new object[schema.Fields.Length];
            _rcfFieldCount = schema.RareChangeFields.Length;
            for (int i = 0; i < schema.NameList.Length; ++i) {
                _nameMap.Add(schema.NameList[i], i);
            }
            _seqIdField = schema.SeqIdIndex;
            _seqIdIsLong = _seqIdField >= 0 && schema.Fields[_seqIdField].Type.Type == TypeTag.Long;
            _blockReader = new(_bufferStream);
        }

        public override int RecordsAffected => throw new NotImplementedException();

        public override string GetName(int i) => _nameMap.First(kvp => kvp.Value == i).Key;

        public override int GetOrdinal(string name) => _nameMap[name];

        private const int BUFFER_LENGTH = 16 * 1024;
        private readonly object[][] _readBuffer = new object[BUFFER_LENGTH][];
        private int _currentRow;
        private int _rowCount;

        public override bool Read()
        {
            if (!CheckForNewBlock())
            {
                Close();
                _onComplete();
                return false;
            }
            _buffer = _readBuffer[_currentRow];
            ++_currentRow;
            return true;
        }

        private bool CheckForNewBlock()
        {
            if (_currentRow >= _rowCount) {
                if (_bufferStream.Position < _bufferStream.Length) {
                    throw new InvalidDataException("Block has not been fully read.");
                }
                if (ReadBlock()) {
                    _currentRow = 0;
                } else {
                    if (_reader.ReadByte() != 0) {
                        throw new InvalidDataException("Final 0 missing at end of stream");
                    }
                    return false;
                }
            }
            return true;
        }

        private bool ReadBlock()
        {
            var size = _reader.Read7BitEncodedInt();
            if (size == 0) {
                return false;
            }
            if (size < 5) { // minimum 1 byte of data + 4-byte CRC checksum
                throw new DataException("Invalid block size");
            }
            var block = _reader.ReadBytes(size);
            ValidateBlockCrc(block);
            _bufferStream.SetLength(0);
            _bufferStream.Write(block, 0, size - sizeof(int));
            _bufferStream.Position = 0;
            FillReadBuffer();
            return true;
        }

        private void FillReadBuffer()
        {
            _rowCount = _blockReader.Read7BitEncodedInt();
            for (int i = 0; i < _rowCount; i++) {
                CheckBufferRow(i);
            }
            var rcfThreshold = _buffer.Length - _rcfFieldCount;
            for (int i = 0; i < _buffer.Length; i++) {
                if (i >= rcfThreshold) {
                    ReadRcfColumn(i);
                } else if (i == _seqIdField) {
                    ReadSeqIdColumn(i);
                } else {
                    ReadRegularColumn(i);
                }
            }
        }

        private void ReadRegularColumn(int column)
        {
            var decoder = _decoders[column];
            for (int i = 0; i < _rowCount; ++i) {
                _readBuffer[i][column] = decoder(_blockReader);
            }
        }

        private void ReadRcfColumn(int column)
        {
            var decoder = _decoders[column];
            int i = 0;
            while (i < _rowCount) {
                var value = decoder(_blockReader);
                var runLength = _blockReader.Read7BitEncodedInt();
                for (int j = i; j < i + runLength; ++j) {
                    _readBuffer[j][column] = value;
                }
                i += runLength;
            }
        }

        private void ReadSeqIdColumn(int column)
        {
            if (_seqIdIsLong) {
                ReadSeqIdLongColumn(column);
            } else {
                ReadSeqIdIntColumn(column);
            }
        }

        private void ReadSeqIdIntColumn(int column)
        {
            var last = _blockReader.Read7BitEncodedInt();
            _readBuffer[0][column] = last;
            for (int i = 1; i < _rowCount; ++i) {
                var diff = _blockReader.Read7BitEncodedInt();
                _readBuffer[i][column] = last + diff;
                last += diff;
            }
        }

        private void ReadSeqIdLongColumn(int column)
        {
            var last = _blockReader.Read7BitEncodedInt64();
            _readBuffer[0][column] = last;
            for (int i = 1; i < _rowCount; ++i) {
                var diff = _blockReader.Read7BitEncodedInt64();
                _readBuffer[i][column] = last + diff;
                last += diff;
            }
        }

        private void CheckBufferRow(int idx)
        {
            var row = _readBuffer[idx];
            var rowSize = _buffer.Length;
            if (row == null) {
                row = new object[rowSize];
                _readBuffer[idx] = row;
            } else if (row.Length != rowSize) {
                throw new DataException("Inconsistent row size in StreamingReader.");
            }
        }

        private static void ValidateBlockCrc(byte[] row)
        {
            if (!Force.Crc32.Crc32CAlgorithm.IsValidWithCrcAtEnd(row)) {
                throw new InvalidDataException("Data block CRC check failed.");
            }
        }

        public override void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}