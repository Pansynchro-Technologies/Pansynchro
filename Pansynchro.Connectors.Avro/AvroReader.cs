using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.Avro
{
    public class AvroReader : IReader, ISourcedConnector, IRandomStreamReader
    {
        private IDataSource? _source;

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            await foreach (var (_, stream) in _source.GetDataAsync()) {
                using var reader = AvroContainer.CreateGenericReader(stream, false);
                var schema = (RecordSchema)reader.Schema;
                yield return new DataStream(new(schema.Namespace, schema.Name), StreamSettings.None, new AvroDataReader(reader));
            }
        }

        public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
        {
            throw new NotImplementedException();
        }

        public Task<Exception?> TestConnection() => Task.FromResult<Exception?>(null);

        public Task<IDataReader> ReadStream(DataDictionary source, string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadStream");
            }
            var stream = source.GetStream(name);
            var readers = _source.GetDataAsync(name)
                .Select(s => new AvroDataReader(AvroContainer.CreateGenericReader(s)))
                .ToEnumerable();
            return Task.FromResult<IDataReader>(new GroupingReader(readers));
        }

        public void Dispose()
        {
            (_source as IDisposable)?.Dispose();
            GC.SuppressFinalize(this);
        }

        private class AvroDataReader : ArrayReader
        {
            private readonly IAvroReader<object> _reader;
            private readonly RecordSchema _schema;
            private IAvroReaderBlock<object>? _block;
            private IEnumerator<AvroRecord> _enumerator = null!;
            private readonly Func<AvroRecord, object[]> _interpreter;

            public AvroDataReader(IAvroReader<object> reader)
            {
                _reader = reader;
                _schema = (RecordSchema)reader.Schema;
                _buffer = new object[_schema.Fields.Count];
                for (int i = 0; i < _schema.Fields.Count; i++) {
                    _nameMap[_schema.Fields[i].Name] = i;
                }
                _interpreter = BuildInterpreter(_schema);
            }

            public override int RecordsAffected => throw new NotImplementedException();

            public override string GetName(int i) => _schema.Fields[i].Name;

            public override bool Read()
            {
                while (_block == null || !_enumerator.MoveNext()) {
                    if (!NextBlock()) {
                        return false;
                    }
                }
                _buffer = _interpreter(_enumerator.Current);
                return true;
            }

            private Func<AvroRecord, object[]> BuildInterpreter(RecordSchema schema)
            {
                var indices = schema.Fields.IndexWhere(f => f.TypeSchema is EnumSchema).ToArray();
                if (indices.Length == 0) {
                    return this.ReadRecord;
                }
                Action<object[]> interpreter = null!;
                foreach (var i in indices) {
                    interpreter += BuildEnumInterpreter(i);
                }
                return r => {
                    var result = ReadRecord(r);
                    interpreter(result);
                    return result;
                };
            }

            private object[] ReadRecord(AvroRecord item)
            {
                var result = new object[_schema.Fields.Count];
                for (var i = 0; i < _schema.Fields.Count; i++) {
                    result[i] = item[i];
                }
                return result;
            }


            private static Action<object[]> BuildEnumInterpreter(int i) =>
                arr => arr[i] = ((AvroEnum)arr[i]).IntegerValue;

            private bool NextBlock()
            {
                _enumerator?.Dispose();
                _block?.Dispose();
                if (!_reader.MoveNext()) {
                    return false;
                }
                _block = _reader.Current;
                _enumerator = _block.Objects.Cast<AvroRecord>().GetEnumerator();
                return true;
            }

            public override Type? GetFieldType(int i)
            {
                var result = _schema.Fields[i].TypeSchema.RuntimeType;
                return result == typeof(AvroEnum) ? typeof(int) : result;
            }

            public override void Dispose()
            {
                _reader.Dispose();
                GC.SuppressFinalize(this);
            }
        }
    }
}