using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Avro
{
    public class AvroAnalyzer : ISchemaAnalyzer, ISourcedConnector
    {
        private IDataSource? _source;

        private Dictionary<string, FieldType> _customTypes = null!;

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }

        public async ValueTask<DataDictionary> AnalyzeAsync(string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
            }
            _customTypes = new();
            await foreach (var (_, stream) in _source.GetDataAsync()) {
                using var reader = AvroContainer.CreateGenericReader(stream, false);
                var schema = reader.Schema;
                if (schema is RecordSchema rs) {
                    return AnalyzeRecordSchema(rs);
                }
                throw new NotSupportedException($"Schema type {reader.Schema.GetType().Name} is not supported yet.");
            }
            return null!;
        }

        private DataDictionary AnalyzeRecordSchema(RecordSchema rs)
        {
            var fields = rs.Fields.Select(AnalyzeRecordField).ToArray();
            var name = new StreamDescription(rs.Namespace, rs.Name);
            var sd = new StreamDefinition(name, fields, Array.Empty<string>());
            return new DataDictionary(rs.FullName, new StreamDefinition[] { sd }) { CustomTypes = _customTypes };
        }

        private FieldDefinition AnalyzeRecordField(RecordField field)
        {
            var name = field.FullName;
            var type = AnalyzeType(field.TypeSchema);
            return new FieldDefinition(name, type);
        }

        private FieldType AnalyzeType(TypeSchema schema)
        {
            if (schema is ArraySchema asc) {
                return AnalyzeArrayType(asc);
            }
            if (schema is UnionSchema usc) {
                return AnalyzeUnionType(usc);
            }
            if (schema is EnumSchema esc) {
                return AnalyzeEnumSchema(esc);
            }
            var tt = schema.RuntimeType.FullName switch {
                "System.Int32" => TypeTag.Int,
                "System.Int64" => TypeTag.Long,
                "System.String" =>TypeTag.Ntext,
                "System.Boolean" => TypeTag.Boolean,
                "System.Single" => TypeTag.Single,
                "System.Double" => TypeTag.Double,
                "System.Byte[]" => TypeTag.Blob,
                _ => throw new NotSupportedException($"Schema field type {schema.RuntimeType.FullName} is not supported yet.")
            };
            return new FieldType(tt, false, CollectionType.None, null);
        }

        private FieldType AnalyzeEnumSchema(EnumSchema esc)
        {
            var values = esc.Symbols
                .Select(s => KeyValuePair.Create(s, esc.GetValueBySymbol(s)))
                .ToArray();
            _customTypes.Add(esc.FullName, new FieldType(TypeTag.Int, false, CollectionType.None, null));
            return new FieldType(TypeTag.Custom, false, CollectionType.None, esc.FullName);
        }

        private FieldType AnalyzeUnionType(UnionSchema usc)
        {
            if (usc.Schemas.Count == 2 && usc.Schemas.OfType<NullSchema>().Count() == 1) {
                var baseType = AnalyzeType(usc.Schemas.Single(s => s is not NullSchema));
                return baseType with { Nullable = true };
            }
            throw new NotSupportedException($"Schema union types other than nullables are not supported yet.");
        }

        private FieldType AnalyzeArrayType(ArraySchema asc)
        {
            var baseType = AnalyzeType(asc.ItemSchema);
            return baseType with { CollectionType = CollectionType.Array };
        }
    }
}
