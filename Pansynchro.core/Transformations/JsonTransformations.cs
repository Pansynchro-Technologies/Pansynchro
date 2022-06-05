using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Newtonsoft.Json.Linq;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.Transformations
{
    public static class JsonTransformations
    {
        public static IDataReader ExtractToSchema(JToken value, StreamDefinition schema, Dictionary<string, string> nameLookup)
            => new EnumerableArrayReader(DoExtractToSchema(value, schema, nameLookup), schema);
        
        private static IEnumerable<object?[]> DoExtractToSchema(JToken value, StreamDefinition schema, Dictionary<string, string> nameLookup)
        {
            if (value is JArray arr) {
                return ExtractArrToSchema(arr, schema, nameLookup);
            }
            if (value is JObject obj) {
                return ExtractObjToSchema(obj, schema, nameLookup);
            }
            if (schema.Fields.Length != 1) {
                throw new DataException($"Single JSON value cannot be extracted to a stream schema with {schema.Fields.Length} fields.");
            }
            return ExtractValueToSchema(value, schema);
        }

        private static IEnumerable<object?[]> ExtractArrToSchema(JArray arr, StreamDefinition schema, Dictionary<string, string> nameLookup)
        {
            var nameOrder = BuildNameOrder(schema.Fields.Select(f => f.Name), nameLookup);
            var extractors = schema.Fields.Select(BuildExtractor);
            var processors = nameOrder.Zip(extractors, KeyValuePair.Create).ToArray();
            if (processors.Length > 1) {
                foreach (var row in arr) {
                    if (row is JObject obj) {
                        yield return ProcessJObject(obj, processors);
                    } else {
                        throw new DataException($"JSON object expected but found {row.Type} for stream {schema.Name}");
                    }
                }
            } else {
                foreach (var row in arr) {
                    if (row is JObject obj) {
                        yield return ProcessJObject(obj, processors);
                    } else {
                        yield return ProcessJValue(row, processors[0].Value);
                    }
                }
            }
        }

        private static IEnumerable<object[]> ExtractObjToSchema(JObject obj, StreamDefinition schema, Dictionary<string, string> nameLookup)
        {
            var nameOrder = BuildNameOrder(schema.Fields.Select(f => f.Name), nameLookup);
            var extractors = schema.Fields.Select(BuildExtractor);
            var processors = nameOrder.Zip(extractors, KeyValuePair.Create).ToArray();
            return new object[][] { ProcessJObject(obj, processors) };
        }

        private static IEnumerable<object?[]> ExtractValueToSchema(JToken value, StreamDefinition schema)
        {
            var extractor = BuildExtractor(schema.Fields[0]);
            return new object?[][] { ProcessJValue(value, extractor) };
        }

        private static IEnumerable<Func<JObject, JToken?>> BuildNameOrder(IEnumerable<string> names, Dictionary<string, string> nameLookup)
        {
            foreach (var name in names) {
                if (nameLookup == null || !nameLookup.TryGetValue(name, out var fieldName)) {
                    fieldName = name;
                }
                yield return obj => obj[fieldName];
            }
        }

        private static object[] ProcessJObject(JObject obj, KeyValuePair<Func<JObject, JToken>, Func<JToken, object>>[] processors)
        {
            var result = new object[processors.Length];
            for (int i = 0; i < processors.Length; ++i) {
                var pair = processors[i];
                result[i] = pair.Value(pair.Key(obj));
            }
            return result;
        }

        private static object?[] ProcessJValue(JToken row, Func<JToken, object?> processor)
        {
            var result = new object?[1] { processor(row) };
            return result;
        }

        private static Func<JToken, object?> BuildExtractor(FieldDefinition field)
        {
            var type = field.Type;
            Func<JToken, object> resultBase = type.Type switch
            {
                TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar
                    or TypeTag.Ntext => j => (string)j!,
                TypeTag.Boolean => j => (bool)j,
                TypeTag.Byte => j => (byte)j,
                TypeTag.Short => j => (short)j,
                TypeTag.Int => j => (int)j,
                TypeTag.Long => j => (long)j,
                TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney => j => (decimal)j,
                TypeTag.Float or TypeTag.Single => j => (float)j,
                TypeTag.Double => j => (double)j,
                TypeTag.Date or TypeTag.Time or TypeTag.DateTime or TypeTag.SmallDateTime => j => DateTime.Parse((string)j!),
                TypeTag.TimeTZ or TypeTag.DateTimeTZ => j => DateTimeOffset.Parse((string)j!),
                TypeTag.Guid => j => Guid.Parse((string)j!),
                TypeTag.Json => j => j,
                _ => throw new NotImplementedException($"No JSON extractor implemented for '{type.Type}'.")
            };
            var resultColl = type.CollectionType switch
            {
                CollectionType.None => resultBase,
                CollectionType.Array => MakeArrayExtractor(resultBase),
                CollectionType.Multiset => throw new NotImplementedException(),
                _ => throw new NotImplementedException($"Invalid collection type '{type.CollectionType}.'"),
            };
            Func<JToken, object?> result = type.Nullable
                ? (j => j == null ? null : resultColl(j))
                : (j => j == null ? throw new ArgumentNullException($"Null/missing JSON values are not allowed for field '${field.Name}.'") : resultColl(j));
            return result;
        }

        private static Func<JToken, object> MakeArrayExtractor(Func<JToken, object> processor) =>
            j => { 
                return j is JArray arr
                    ? arr.Select(processor).ToArray()
                    : throw new DataException($"Provided JSON value is not an array");
            };
    }
}
