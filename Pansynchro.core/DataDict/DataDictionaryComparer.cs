using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;

namespace Pansynchro.Core.DataDict
{
    public record HarmonizedDictionary(
        DataDictionary Source,
        DataDictionary Dest,
        ITransformer? Transformer,
        string[] Errors);

    public class DataDictionaryComparer
    {
        public HarmonizedDictionary Harmonize(DataDictionary source, DataDictionary dest, NameStrategyType nst)
        {
            var strategy = NameStrategy.Get(nst);
            var nameLookup = source.Streams.ToDictionary(s => s.Name.ToString(), s => strategy.MatchingName(s.Name));
            source = Prune(source, dest, strategy);
            dest = Prune(dest, source, strategy);
            var result = Compare(source, dest, strategy);
            var conversions = result.OfType<ConversionLine>().ToLookup(l => l.Message[..^1]);
            var errors = result.OfType<ComparisonError>().Select(e => e.ToString()).ToArray();
            var transformer = Transform(conversions, nameLookup, source, dest);
            return new HarmonizedDictionary(source, dest, transformer, errors);
        }

        private static DataDictionary Prune(DataDictionary source, DataDictionary dest, NameStrategy strategy)
        {
            var destNames = dest.Streams.Select(s => strategy.MatchingName(s.Name)).ToHashSet(StringComparer.InvariantCultureIgnoreCase);
            var validStreams = source.Streams
                .Where(s => destNames.Contains(strategy.MatchingName(s.Name)))
                .Select(s => PruneStream(s, dest.GetStream(s.Name, strategy)))
                .ToArray();
            var streamNames = validStreams.Select(s => s.Name).ToHashSet();
            var validDeps = source.DependencyOrder
                .Select(d => ValidateDeps(d, streamNames))
                .Where(d => d.Length > 0)
                .ToArray();
            return source with { Streams = validStreams, DependencyOrder = validDeps };
        }

        private static StreamDescription[] ValidateDeps(StreamDescription[] d, HashSet<StreamDescription> streamNames)
            => d.Where(n => streamNames.Contains(n)).ToArray();

        private static StreamDefinition PruneStream(StreamDefinition source, StreamDefinition comparison)
        {
            var valid = source.NameList.Intersect(comparison.NameList, StringComparer.InvariantCultureIgnoreCase).ToArray();
            if (valid.Length == source.Fields.Length && valid.Length == comparison.Fields.Length) {
                return source with { Fields = source.Fields.OrderBy(f => f.Name).ToArray() };
            }
            var validFields = source.Fields.Where(f => valid.Contains(f.Name)).OrderBy(f => f.Name).ToArray();
            return source with { Fields = validFields };
        }

        private static ComparisonResult[] Compare(DataDictionary source, DataDictionary dest, NameStrategy strategy)
        {
            Debug.Assert(source.Streams.Length == dest.Streams.Length);
            LowerCustomTypes(source);
            LowerCustomTypes(dest);
            var sources = source.Streams.ToDictionary(s => strategy.MatchingName(s.Name));
            var dests = dest.Streams.ToDictionary(s => strategy.MatchingName(s.Name));
            return TypeCheck(sources, dests).ToArray();
        }

        private static void LowerCustomTypes(DataDictionary dict)
        {
            foreach (var stream in dict.Streams) {
                for (int i = 0; i < stream.Fields.Length; ++i) {
                    var field = stream.Fields[i];
                    if (field.Type.Type == TypeTag.Custom) {
                        stream.Fields[i] = field with { Type = dict.CustomTypes[field.Type.Info!] with { Nullable = field.Type.Nullable } };
                    }
                }
            }
        }

        private static IEnumerable<ComparisonResult> TypeCheck(Dictionary<string, StreamDefinition> source, Dictionary<string, StreamDefinition> dest)
        {
            foreach (var pair in source) {
                var destStream = dest[pair.Key];
                foreach (var result in TypeCheckStream(pair.Value, destStream)) {
                    yield return result;
                }
            }
        }

        private static IEnumerable<ComparisonResult> TypeCheckStream(StreamDefinition stream, StreamDefinition destStream)
        {
            var fields = stream.Fields.ToDictionary(f => f.Name, f => f.Type);
            var destFields = destStream.Fields.ToDictionary(f => f.Name, f => f.Type, StringComparer.OrdinalIgnoreCase);
            Debug.Assert(fields.Count == destFields.Count);
            foreach (var pair in fields) {
                var destField = destFields[pair.Key];
                var result = TypeCheckField(pair.Value, destField, pair.Key);
                if (result != null)
                    yield return result with { Message = $"{stream.Name}.{result.Message}" };
            }
        }

        public static ComparisonResult? TypeCheckField(FieldType srcField, FieldType destField, string fieldName)
        {
            if (srcField == destField || srcField.CanAssignNotNullToNull(destField) || srcField.CanAssignSpecificToGeneral(destField)) {
                return null;
            }
            if (srcField.Nullable && !destField.Nullable) {
                return new ComparisonError($"{fieldName}: Can't sync nullable source ({srcField}) to NOT NULL destination ({destField}).");
            }
            if (srcField.CollectionType != destField.CollectionType) {
                return new ComparisonError($"{fieldName}: Can't sync collection type {srcField.CollectionType} to destination collection type {destField.CollectionType}.");
            }
            var result = CheckTypeConvertible(srcField, destField, fieldName);
            if (result != null) {
                return result;
            }
            if (_implicits.TryGetValue(srcField.Type, out var candidates) && candidates.Contains(destField.Type)) {
                return null;
            }
            if (srcField.Type != destField.Type && (srcField.Info == destField.Info || destField.Info == null || _stringTypes.Contains(destField.Type))) {
                return CheckTypePromotable(srcField.Type, destField.Type, fieldName);
            }
            return new ComparisonError($"{fieldName}: '{srcField}' is different from '{destField}'.");
        }

        private static ConversionLine? CheckTypeConvertible(FieldType source, FieldType dest, string name)
        {
            if (source.Type == TypeTag.Guid
                && dest.Type is TypeTag.Binary or TypeTag.Varbinary
                && (dest.Info == null || int.Parse(dest.Info, CultureInfo.InvariantCulture) >= 16)) {
                return new NamedConversionLine(name, "GuidToBinary");
            }
            if (source.Type == TypeTag.Boolean
                && dest.Type == TypeTag.Bits
                && dest.Info == "1") {
                return new PromotionLine(name, TypeTag.Byte);
            }
            if (source.Type == TypeTag.HierarchyID
                && dest.Type is TypeTag.Varchar or TypeTag.Nvarchar
                && dest.Info is not null && int.Parse(dest.Info) >= 16) {
                return new PromotionLine(name, dest.Type);
            }
            return null;
        }

        private static readonly Dictionary<TypeTag, TypeTag[]> _implicits = new() {
            { TypeTag.Varchar, new[] { TypeTag.Nvarchar, TypeTag.Text, TypeTag.Ntext } },
            { TypeTag.Nvarchar, new[] { TypeTag.Ntext } },
            { TypeTag.Varbinary, new[] { TypeTag.Blob } },
            { TypeTag.Date, new[] { TypeTag.DateTime } },
            { TypeTag.Time, new[] { TypeTag.Time } }, //workaround for differing precision limits on different databases
        };

        private static readonly Dictionary<TypeTag, TypeTag[]> _promotables = new() {
            { TypeTag.Boolean, new[] { TypeTag.Bits } },
            { TypeTag.Byte, new[] { TypeTag.Short, TypeTag.Int, TypeTag.Long } },
            { TypeTag.Short, new[] { TypeTag.Int, TypeTag.Long } },
            { TypeTag.Int, new[] { TypeTag.Long } },
            { TypeTag.SmallMoney, new[] { TypeTag.Money, TypeTag.Numeric, TypeTag.Double } },
            { TypeTag.Money, new[] { TypeTag.Numeric, TypeTag.Double } },
            { TypeTag.Decimal, new[] { TypeTag.Numeric } },
        };

        private static readonly TypeTag[] _stringTypes = new[] { TypeTag.Varchar, TypeTag.Nvarchar, TypeTag.Text, TypeTag.Ntext, TypeTag.Unstructured };

        private static ComparisonResult CheckTypePromotable(TypeTag source, TypeTag dest, string name)
        {
            if (_promotables.TryGetValue(source, out var candidates) && candidates.Contains(dest)) {
                return new PromotionLine(name, dest);
            }
            if (_stringTypes.Contains(dest)) {
                return new PromotionLine(name, dest);
            }
            return new ComparisonError($"{name}: Can't promote '{source}' type to '{dest}'.");
        }

        protected virtual ITransformer? Transform(ILookup<string, ConversionLine> conversions, Dictionary<string, string> nameLookup, DataDictionary source, DataDictionary dest)
        {
            if (conversions.Count == 0) {
                return null;
            }
            return new ComparisonTransformer(conversions, nameLookup, source);
        }

        private class ComparisonTransformer : StreamTransformerBase
        {
            public ComparisonTransformer(
                ILookup<string, ConversionLine> conversions,
                Dictionary<string, string> nameLookup,
                DataDictionary schema) : base(schema)
            {
                foreach (var pair in nameLookup) {
                    if (pair.Key != pair.Value) {
                        _nameMap.Add(StreamDescription.Parse(pair.Key), StreamDescription.Parse(pair.Value));
                    }
                }
                foreach (var group in conversions) {
                    _streamDict.Add(group.Key, BuildConversion(group, nameLookup[group.Key], schema.GetStream(group.Key)));
                }
            }

            private Func<IDataReader, IEnumerable<object[]>> BuildConversion(
                IGrouping<string, ConversionLine> group, string destName, StreamDefinition schema)
            {
                Action<object[]> action = null!;
                foreach (var line in group) {
                    action += line switch {
                        PromotionLine pLine => PromotionConversion(pLine, schema),
                        NamedConversionLine nLine => NamedConversion(nLine, schema),
                        _ => throw new ArgumentException($"Unknown conversion line type: {line.GetType().Name}")
                    };
                }
                return Impl;
                IEnumerable<object[]> Impl(IDataReader source)
                {
                    try { 
                        var dest = new object[source.FieldCount];
                        while (source.Read()) {
                            source.GetValues(dest);
                            action(dest);
                            yield return dest;
                        }
                    } finally {
                        source.Dispose();
                    }
                };
            }

            private static Action<object[]> NamedConversion(NamedConversionLine line, StreamDefinition schema)
            {
                var idx = Array.IndexOf(schema.NameList, line.Field);
                return line.ConversionName switch {
                    "GuidToBinary" => a => a[idx] = ((Guid)a[idx]).ToByteArray(),
                    _ => throw new ArgumentException($"Unknown conversion '{line.ConversionName}.'")
                };
            }

            private static Dictionary<(TypeTag, TypeTag), Func<object, object>> _promotions = new() {
                { (TypeTag.Byte, TypeTag.Short), o => (short)(byte)o },
                { (TypeTag.Byte, TypeTag.Int), o => (int)(byte)o },
                { (TypeTag.Byte, TypeTag.Long), o => (long)(byte)o },
                { (TypeTag.Short, TypeTag.Int), o => (int)(short)o },
                { (TypeTag.Short, TypeTag.Long), o => (long)(short)o },
                { (TypeTag.Int, TypeTag.Long), o => (long)(int)o },
            };

            private static Func<object, object> GetPromotion(TypeTag srcType, TypeTag dstType)
            {
                if (_stringTypes.Contains(dstType))
                    return o => o.ToString()!;
                if (_promotions.TryGetValue((srcType, dstType), out var result))
                    return result;
                throw new ArgumentException($"No promotion defined for {srcType} -> {dstType}.");
            }

            private static Action<object[]> PromotionConversion(PromotionLine line, StreamDefinition schema)
            {
                var idx = Array.IndexOf(schema.NameList, line.Field);
                var srcField = schema.Fields[idx];
                var conversion = GetPromotion(srcField.Type.Type, line.NewType);
                return a => a[idx] = conversion(a[idx]);
            }
        }
    }
}
