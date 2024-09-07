using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json.Nodes;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.Transformations
{
	public static class JsonTransformations
	{
		public static IDataReader ExtractToSchema(JsonNode value, StreamDefinition schema, Dictionary<string, string> nameLookup)
			=> new EnumerableArrayReader(DoExtractToSchema(value, schema, nameLookup), schema);
		
		private static IEnumerable<object?[]> DoExtractToSchema(JsonNode value, StreamDefinition schema, Dictionary<string, string> nameLookup)
		{
			if (value is JsonArray arr) {
				return ExtractArrToSchema(arr, schema, nameLookup);
			}
			if (value is JsonObject obj) {
				return ExtractObjToSchema(obj, schema, nameLookup);
			}
			if (schema.Fields.Length != 1) {
				throw new DataException($"Single JSON value cannot be extracted to a stream schema with {schema.Fields.Length} fields.");
			}
			return ExtractValueToSchema((JsonValue)value, schema);
		}

		private static IEnumerable<object?[]> ExtractArrToSchema(JsonArray arr, StreamDefinition schema, Dictionary<string, string> nameLookup)
		{
			var nameOrder = BuildNameOrder(schema.Fields.Select(f => f.Name), nameLookup);
			var extractors = schema.Fields.Select(BuildExtractor);
			var processors = nameOrder.Zip(extractors, KeyValuePair.Create).ToArray();
			if (processors.Length > 1) {
				foreach (var row in arr) {
					if (row is JsonObject obj) {
						yield return ProcessJObject(obj, processors);
					} else {
						throw new DataException($"JSON object expected but found {row?.GetType()?.Name ?? "null"} for stream {schema.Name}");
					}
				}
			} else {
				foreach (var row in arr) {
					if (row is JsonObject obj) {
						yield return ProcessJObject(obj, processors);
					} else {
						yield return ProcessJValue(row, processors[0].Value);
					}
				}
			}
		}

		private static object[][] ExtractObjToSchema(JsonObject obj, StreamDefinition schema, Dictionary<string, string> nameLookup)
		{
			var nameOrder = BuildNameOrder(schema.Fields.Select(f => f.Name), nameLookup);
			var extractors = schema.Fields.Select(BuildExtractor);
			var processors = nameOrder.Zip(extractors, KeyValuePair.Create).ToArray();
			return new object[][] { ProcessJObject(obj, processors) };
		}

		private static object?[][] ExtractValueToSchema(JsonValue value, StreamDefinition schema)
		{
			var extractor = BuildExtractor(schema.Fields[0]);
			return new object?[][] { ProcessJValue(value, extractor) };
		}

		private static IEnumerable<Func<JsonObject, JsonNode?>> BuildNameOrder(IEnumerable<string> names, Dictionary<string, string> nameLookup)
		{
			foreach (var name in names) {
				if (nameLookup == null || !nameLookup.TryGetValue(name, out var fieldName)) {
					fieldName = name;
				}
				yield return obj => obj[fieldName];
			}
		}

		private static object[] ProcessJObject(JsonObject obj, KeyValuePair<Func<JsonObject, JsonNode?>, Func<JsonNode?, object>>[] processors)
		{
			var result = new object[processors.Length];
			for (int i = 0; i < processors.Length; ++i) {
				var pair = processors[i];
				result[i] = pair.Value(pair.Key(obj));
			}
			return result;
		}

		private static object?[] ProcessJValue(JsonNode? row, Func<JsonNode?, object?> processor)
		{
			var result = new object?[1] { processor(row) };
			return result;
		}

		private static Func<JsonNode?, object> BuildExtractor(FieldDefinition field)
		{
			var type = field.Type;
			Func<JsonNode, object> resultBase = type.Type switch {
				TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar 
					or TypeTag.Ntext => j => j.GetValue<string>(),
				TypeTag.Boolean => j => j.GetValue<bool>(),
				TypeTag.Byte => j => j.GetValue<byte>(),
				TypeTag.Short => j => j.GetValue<short>(),
				TypeTag.Int => j => j.GetValue<int>(),
				TypeTag.Long => j => j.GetValue<long>(),
				TypeTag.UShort => j => j.GetValue<ushort>(),
				TypeTag.UInt => j => j.GetValue<uint>(),
				TypeTag.ULong => j => j.GetValue<ulong>(),
				TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney => j => j.GetValue<decimal>(),
				TypeTag.Float or TypeTag.Single => j => j.GetValue<float>(),
				TypeTag.Double => j => j.GetValue<double>(),
				TypeTag.Date or TypeTag.Time or TypeTag.DateTime or TypeTag.SmallDateTime => j => j.GetValue<DateTime>(),
				TypeTag.TimeTZ or TypeTag.DateTimeTZ => j => j.GetValue<DateTimeOffset>(),
				TypeTag.Guid => j => j.GetValue<Guid>(),
				TypeTag.Json => j => j,
				_ => throw new NotImplementedException($"No JSON extractor implemented for '{type.Type}'.")
			};
			var resultColl = type.CollectionType switch {
				CollectionType.None => resultBase,
				CollectionType.Array => MakeArrayExtractor(resultBase!),
				CollectionType.Multiset => throw new NotImplementedException(),
				_ => throw new NotImplementedException($"Invalid collection type '{type.CollectionType}.'"),
			};
			Func<JsonNode?, object> result = type.Nullable
				? (j => j == null ? DBNull.Value : resultColl(j))
				: (j => j == null ? throw new ArgumentNullException($"Null/missing JSON values are not allowed for field '${field.Name}.'") : resultColl(j));
			return result;
		}

		private static Func<JsonNode?, object> MakeArrayExtractor(Func<JsonNode?, object> processor) =>
			j => { 
				return j is JsonArray arr
					? arr.Select(processor).ToArray()
					: throw new DataException($"Provided JSON value is not an array");
			};
	}
}
