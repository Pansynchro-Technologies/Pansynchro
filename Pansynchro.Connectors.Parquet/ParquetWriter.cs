using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Parquet;
using Parquet.Data;
using PWriter = Parquet.ParquetWriter;
using PSchema = Parquet.Data.Schema;
using DataColumn = Parquet.Data.DataColumn;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.Parquet
{
    public class ParquetWriter : IWriter, ISinkConnector
    {
        private IDataSink? _sink;

        public void SetDataSink(IDataSink sink)
        {
            _sink = sink;
        }

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            if (_sink == null) {
                throw new DataException("Must call SetDataSink before calling Sync");
            }
            EventLog.Instance.AddStartSyncEvent();
            await foreach (var (name, settings, reader) in streams) {
                EventLog.Instance.AddStartSyncStreamEvent(name);
                try {
                    var streamDef = dest.GetStream(name, NameStrategy.Get(NameStrategyType.Identity));
                    var (schema, writers) = ParquetWriter.BuildSchema(streamDef);
                    using var writer = new PWriter(schema, await _sink.WriteData(name.ToString()));
                    using var group = writer.CreateRowGroup();
                    ParquetWriter.WriteParquetData(reader, writers, group);
                } catch (Exception ex) {
                    EventLog.Instance.AddErrorEvent(ex, name);
                    if (!ErrorManager.ContinueOnError)
                        throw;
                } finally {
                    reader.Dispose();
                }
                EventLog.Instance.AddEndSyncStreamEvent(name);
            }
            EventLog.Instance.AddEndSyncEvent();
        }

        private static void WriteParquetData(
            IDataReader reader, Func<object[][], DataColumn>[] writers, ParquetRowGroupWriter group)
        {
            var len = writers.Length;
            var list = new List<object[]>();
            while (reader.Read()) {
                var buffer = new object[len];
                reader.GetValues(buffer);
                list.Add(buffer);
            }
            var grid = list.ToArray();
            foreach (var writer in writers) {
                var column = writer(grid);
                group.WriteColumn(column);
            }
        }

        private static (PSchema schema, Func<object[][], DataColumn>[] writers) BuildSchema(
            StreamDefinition streamDef)
        {
            var fields = new List<Field>();
            var writers = new List<Func<object[][], DataColumn>>();
            for (int i = 0; i < streamDef.Fields.Length; ++i) {
                var field = BuildField(streamDef.Fields[i]);
                fields.Add(field);
                writers.Add(ParquetWriter.BuildWriter(streamDef.Fields[i].Type, field, i));
            }
            var schema = new PSchema(fields);
            return (schema, writers.ToArray());
        }

        private static Func<object[][], DataColumn> BuildWriter(FieldType ft, DataField field, int idx)
        {
            var arrayBuilder = MakeArrayBuilder(ft, idx);
            return grid => new DataColumn(field, arrayBuilder(grid));
        }

        private static Func<object[][], Array> MakeArrayBuilder(FieldType ft, int idx)
        {
            var nullable = ft.Nullable;
            return ft.Type switch {
                TypeTag.Boolean => nullable ? DoMakeNullableArrayBuilder<bool>(idx) : DoMakeArrayBuilder<bool>(idx),
                TypeTag.Byte => nullable ? DoMakeNullableArrayBuilder<byte>(idx) : DoMakeArrayBuilder<byte>(idx),
                TypeTag.SByte => nullable ? DoMakeNullableArrayBuilder<sbyte>(idx) : DoMakeArrayBuilder<sbyte>(idx),
                TypeTag.Short => nullable ? DoMakeNullableArrayBuilder<short>(idx) : DoMakeArrayBuilder<short>(idx),
                TypeTag.UShort => nullable ? DoMakeNullableArrayBuilder<ushort>(idx) : DoMakeArrayBuilder<ushort>(idx),
                TypeTag.Int => nullable ? DoMakeNullableArrayBuilder<int>(idx) : DoMakeArrayBuilder<int>(idx),
                TypeTag.UInt => nullable ? DoMakeNullableArrayBuilder<uint>(idx) : DoMakeArrayBuilder<uint>(idx),
                TypeTag.Long => nullable ? DoMakeNullableArrayBuilder<long>(idx) : DoMakeArrayBuilder<long>(idx),
                TypeTag.ULong => nullable ? DoMakeNullableArrayBuilder<ulong>(idx) : DoMakeArrayBuilder<ulong>(idx),
                TypeTag.Blob or TypeTag.Binary or TypeTag.Varbinary =>
                    nullable ? DoMakeNullableArrayRefBuilder<byte[]>(idx) : DoMakeArrayBuilder<byte[]>(idx),
                TypeTag.Ntext or TypeTag.Text or TypeTag.Varchar or TypeTag.Nvarchar or TypeTag.Char =>
                    nullable ? DoMakeNullableArrayRefBuilder<string>(idx) : DoMakeArrayBuilder<string>(idx),
                TypeTag.Single or TypeTag.Float => 
                    nullable ? DoMakeNullableArrayBuilder<float>(idx) : DoMakeArrayBuilder<float>(idx),
                TypeTag.Double => nullable ? DoMakeNullableArrayBuilder<double>(idx) : DoMakeArrayBuilder<double>(idx),
                TypeTag.Decimal => nullable ? DoMakeNullableArrayBuilder<decimal>(idx) : DoMakeArrayBuilder<decimal>(idx),
                TypeTag.DateTimeTZ => nullable ? DoMakeNullableArrayBuilder<DateTimeOffset>(idx) : DoMakeArrayBuilder<DateTimeOffset>(idx),
                TypeTag.DateTime => nullable ? DoMakeNullableDateTimeBuilder(idx) : DoMakeDateTimeBuilder(idx),
                TypeTag.Interval => nullable ? DoMakeNullableArrayBuilder<TimeSpan>(idx) : DoMakeArrayBuilder<TimeSpan>(idx),
                _ => throw new NotSupportedException($"Field data type '{ft.Type}' is not supported")
            };
        }

        private static Func<object[][], Array> DoMakeArrayBuilder<T>(int idx)
        {
            return grid => {
                var result = new T[grid.Length];
                for (int i = 0; i < grid.Length; ++i) {
                    result[i] = (T)grid[i][idx];
                }
                return result;
            };
        }

        private static Func<object[][], Array> DoMakeNullableArrayBuilder<T>(int idx) where T: struct
        {
            return grid => {
                var result = new T?[grid.Length];
                for (int i = 0; i < grid.Length; ++i) {
                    var value = grid[i][idx];
                    result[i] = value == null || value == DBNull.Value ? (T?)null : (T)value;
                }
                return result;
            };
        }

        private static Func<object[][], Array> DoMakeDateTimeBuilder(int idx)
        {
            return grid => {
                var result = new DateTimeOffset[grid.Length];
                for (int i = 0; i < grid.Length; ++i) {
                    result[i] = new DateTimeOffset((DateTime)grid[i][idx]);
                }
                return result;
            };
        }

        private static Func<object[][], Array> DoMakeNullableArrayRefBuilder<T>(int idx) where T: class
        {
            return grid => {
                var result = new T?[grid.Length];
                for (int i = 0; i < grid.Length; ++i) {
                    var value = grid[i][idx];
                    result[i] = value == null || value == DBNull.Value ? null : (T)value;
                }
                return result;
            };
        }

        private static Func<object[][], Array> DoMakeNullableDateTimeBuilder(int idx)
        {
            return grid => {
                var result = new DateTimeOffset?[grid.Length];
                for (int i = 0; i < grid.Length; ++i) {
                    var value = grid[i][idx];
                    result[i] = value == null || value == DBNull.Value ? (DateTimeOffset?)null : new DateTimeOffset((DateTime)value);
                }
                return result;
            };
        }

        private static DataField BuildField(FieldDefinition fd)
        {
            var type = fd.Type;
            if (type.CollectionType == CollectionType.Array) {
                throw new DataException("Array types are not supported yet");
            }
            return new DataField(fd.Name, LookupDataType(type.Type), type.Nullable);
        }

        private static DataType LookupDataType(TypeTag type)
            => type switch {
                TypeTag.Boolean => DataType.Boolean,
                TypeTag.Byte => DataType.Byte,
                TypeTag.SByte => DataType.SignedByte,
                TypeTag.Short => DataType.Short,
                TypeTag.UShort => DataType.UnsignedShort,
                TypeTag.Int => DataType.Int32,
                TypeTag.UInt => DataType.UnsignedInt32,
                TypeTag.Long => DataType.Int64,
                TypeTag.ULong => DataType.UnsignedInt64,
                TypeTag.Blob or TypeTag.Binary or TypeTag.Varbinary => DataType.ByteArray,
                TypeTag.Ntext or TypeTag.Text or TypeTag.Varchar or TypeTag.Nvarchar or TypeTag.Char => DataType.String,
                TypeTag.Single or TypeTag.Float => DataType.Float,
                TypeTag.Double => DataType.Double,
                TypeTag.Decimal => DataType.Decimal,
                TypeTag.DateTimeTZ or TypeTag.DateTime => DataType.DateTimeOffset,
                TypeTag.Interval => DataType.TimeSpan,
                _ => throw new NotSupportedException($"Field data type '{type}' is not supported")
            };

        public void Dispose()
        {
            (_sink as IDisposable)?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
