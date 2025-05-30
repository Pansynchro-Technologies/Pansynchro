using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Avro;
using Avro.File;
using Avro.Generic;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Connectors.Avro
{
	public class AvroAnalyzer : ISchemaAnalyzer, ISourcedConnector
	{
		private IDataSource? _source;

		private Dictionary<string, IFieldType> _customTypes = null!;

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
				using var reader = DataFileReader<GenericRecord>.OpenReader(stream);
				var schema = reader.GetSchema();
				if (schema is RecordSchema rs) {
					return AnalyzeRecordSchema(rs);
				}
				throw new NotSupportedException($"Schema type {schema.GetType().Name} is not supported yet.");
			}
			return null!;
		}

		private DataDictionary AnalyzeRecordSchema(RecordSchema rs)
		{
			var fields = rs.Fields.Select(AnalyzeRecordField).ToArray();
			var name = new StreamDescription(rs.Namespace, rs.Name);
			var sd = new StreamDefinition(name, fields, Array.Empty<string>());
			return new DataDictionary(rs.Fullname, new StreamDefinition[] { sd }) { CustomTypes = _customTypes };
		}

		private FieldDefinition AnalyzeRecordField(Field field)
		{
			var name = field.Name;
			var type = AnalyzeType(field.Schema);
			return new FieldDefinition(name, type);
		}

		private IFieldType AnalyzeType(Schema schema)
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
			if (schema.Tag == Schema.Type.Logical) {
				return AnalyzeLogicalTypeSchema(schema) ?? AnalyzeType(((LogicalSchema)schema).BaseSchema);
			}
			var tt = schema.Tag switch {
				Schema.Type.Int => TypeTag.Int,
				Schema.Type.Long => TypeTag.Long,
				Schema.Type.String => TypeTag.Ntext,
				Schema.Type.Boolean => TypeTag.Boolean,
				Schema.Type.Float => TypeTag.Single,
				Schema.Type.Double => TypeTag.Double,
				Schema.Type.Bytes or Schema.Type.Fixed => TypeTag.Blob,
				_ => throw new NotSupportedException($"Schema field type {schema.Tag} is not supported yet.")
			};
			return new BasicField(tt, false, null, false);
		}

		private static IFieldType? AnalyzeLogicalTypeSchema(Schema type)
		{
			switch (type.GetProperty("logicalType")) {
				case "timestamp-millis":
				case "timestamp-micros":
				case "date":
					return new BasicField(TypeTag.DateTime, false, null, false);
				case "time-millis":
				case "time-micros":
					return new BasicField(TypeTag.Time, false, null, false);
				case "decimal":
					return new BasicField(TypeTag.Decimal, false, $"({type.GetProperty("precision")},{type.GetProperty("scale")})", false);
				case "uuid":
					return new BasicField(TypeTag.Guid, false, null, true);
			};
			return null;
		}

		private IFieldType AnalyzeEnumSchema(EnumSchema esc)
		{
			var bf = new BasicField(TypeTag.Int, false, null, false);
			_customTypes.Add(esc.Name, bf);
			return new CustomField(esc.Name, bf, false);
		}

		private static bool IsNullSchema(Schema value)
			=> value is PrimitiveSchema ps && ps.Tag == Schema.Type.Null;

		private IFieldType AnalyzeUnionType(UnionSchema usc)
		{
			if (usc.Schemas.Count == 2 && usc.Schemas.Where(IsNullSchema).Count() == 1) {
				var baseType = AnalyzeType(usc.Schemas.Single(s => !IsNullSchema(s)));
				return baseType.MakeNull();
			}
			throw new NotSupportedException($"Schema union types other than nullables are not supported yet.");
		}

		private IFieldType AnalyzeArrayType(ArraySchema asc)
		{
			var baseType = AnalyzeType(asc.ItemSchema);
			return new CollectionField(baseType, CollectionType.Array, false);
		}
	}
}
