using System;
using System.Collections.Generic;
using System.Data;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.Core.Transformations
{
    public static class LookupTransformations
    {
        public static Dictionary<K, V> CreateLookup<K, V>(IDataReader stream, int keyColumn, int valColumn)
            where K : notnull
        {
            var result = new Dictionary<K, V>();
            while (stream.Read()) {
                if (stream.IsDBNull(keyColumn)) {
                    throw new DataException("Null values are not permitted in the key column of a CreateLookup operation.");
                }
                result.Add((K)stream[keyColumn], (V)stream[valColumn]);
            }
            return result;
        }

        /*
        public static Dictionary<K, object[]> CreateLookup<K>(IDataReader stream, int keyColumn)
        {
            var result = new Dictionary<K, object[]>();
            while (stream.Read()) {
                if (stream.IsDBNull(keyColumn)) {
                    throw new DataException("Null values are not permitted in the key column of a CreateLookup operation.");
                }
                var values = new object[stream.FieldCount];
                stream.GetValues(values);
                result.Add((K)stream[keyColumn], values);
            }
            return result;
        }
        */

        public static IDataReader AddLookup<K, V>
            (IDataReader stream, Dictionary<K, V> lookup, int keyColumn, bool nullable, StreamDefinition definition)
            where K : notnull
            => new TransformingReader(stream, MakeTransformer(lookup, keyColumn, nullable), definition);

        /*
        public static IDataReader AddLookup<K>(IDataReader stream, Dictionary<K, object[]> lookup, int keyColumn, bool nullable)
            => new LookupMergeDataReader<K>(stream, lookup, keyColumn, nullable);
        */

        private static Func<IDataReader, IEnumerable<object?[]>> MakeTransformer<K, V>(Dictionary<K, V> lookup, int keyColumn, bool nullable)
                        where K : notnull
        {
            static IEnumerable<object?[]> Transformer(IDataReader stream, Dictionary<K, V> lookup, int keyColumn, bool nullable)
            {
                var result = new object?[stream.FieldCount + 1];
                while (stream.Read()) {
                    stream.GetValues(result!);
                    if (lookup.TryGetValue((K)result[keyColumn]!, out var newVal)) {
                        result[stream.FieldCount] = newVal;
                    } else if (nullable) {
                        result[stream.FieldCount] = null;
                    } else {
                        throw new DataException($"No matching lookup value found for {result[keyColumn]}.");
                    }
                    yield return result;
                }
            }

            return stream => Transformer(stream, lookup, keyColumn, nullable);
        }
    }
}
