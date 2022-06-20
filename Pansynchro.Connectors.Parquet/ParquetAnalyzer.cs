using System.Data;

using Parquet;
using Parquet.Data;
using PReader = Parquet.ParquetReader;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Parquet
{

    public class ParquetAnalyzer : ISchemaAnalyzer
    {
        private IDataSource? _source;

        public ParquetAnalyzer(string config)
        { }

        public async ValueTask<DataDictionary> AnalyzeAsync(string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
            }
            string? lastName = null;
            var defs = new List<StreamDefinition>();
            await foreach (var (sName, stream) in _source.GetDataAsync()) {
                if (lastName != sName) {
                    defs.Add(AnalyzeFile(sName, stream));
                    lastName = sName;
                } else {
                    stream.Dispose();
                }
            }
            return new DataDictionary(name, defs.ToArray());
        }

        private static StreamDefinition AnalyzeFile(string name, Stream stream)
        {
            using var lStream = ParquetStreamHelper.CheckStream(stream);
            using var reader = new PReader(lStream);
            var fields = reader.Schema.GetDataFields().Select(AnalyzeField).ToArray();
            return new StreamDefinition(new StreamDescription(null, name), fields, Array.Empty<string>());
        }

        private static FieldDefinition AnalyzeField(DataField field)
        {
            var type = field.DataType switch {
                DataType.Boolean => TypeTag.Boolean,
                DataType.Byte or DataType.UnsignedByte => TypeTag.Byte,
                DataType.SignedByte => TypeTag.SByte,
                DataType.Short or DataType.Int16 => TypeTag.Short,
                DataType.UnsignedShort or DataType.UnsignedInt16 => TypeTag.UShort,
                DataType.Int32 => TypeTag.Int,
                DataType.UnsignedInt32 => TypeTag.UInt,
                DataType.Int64 => TypeTag.Long,
                DataType.UnsignedInt64 => TypeTag.ULong,
                DataType.ByteArray => TypeTag.Blob,
                DataType.String => TypeTag.Ntext,
                DataType.Float => TypeTag.Single,
                DataType.Double => TypeTag.Double,
                DataType.Decimal => TypeTag.Decimal,
                DataType.DateTimeOffset => TypeTag.DateTimeTZ,
                DataType.TimeSpan => TypeTag.Interval,
                _ => throw new NotSupportedException($"Parquet field data type '{field.DataType}' is not supported")
            };
            var cType = field.IsArray ? CollectionType.Array : CollectionType.None;
            return new FieldDefinition(field.Name, new FieldType(type, field.HasNulls, cType, null));
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }
    }
}