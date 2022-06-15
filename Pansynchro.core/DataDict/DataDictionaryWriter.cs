using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core.Incremental;
using Pansynchro.Core.Pansync;

namespace Pansynchro.Core.DataDict
{
    public static class DataDictionaryWriter
    {
        public static async Task<string> Write(ISchemaAnalyzer source, string name)
        {
            var schema = await source.AnalyzeAsync(name);
            return Write(schema);
        }

        public static string Write(DataDictionary schema)
        {
            Command result = SerializeSchema(schema);
            return result.ToString();
        }

        private static Command SerializeSchema(DataDictionary schema)
        {
            var body = new List<Statement>();
            foreach (var stream in schema.Streams)
                body.Add(SerializeStream(stream));
            body.Add(SerializeDeps(schema.DependencyOrder));
            body.Add(SerializeCustomTypes(schema.CustomTypes));
            if (schema.Incremental.Count > 0) {
                body.Add(SerializeIncrementalData(schema.Incremental));
            }
            var result = new Command("DataDictionary", new[] { WriteName(schema.Name) }, null, body.ToArray());
            return result;
        }

        private static Statement SerializeIncrementalData(Dictionary<StreamDescription, IncrementalStrategy> data)
        {
            var lookup = data.ToLookup(kvp => kvp.Value, kvp => kvp.Key);
            var args = new List<Expression>();
            foreach (var group in lookup) {
                var list = new List<Expression>();
                list.AddRange(group.Select(sd => WriteName(sd.ToString())));
                args.Add(new NamedListNode(new NameNode(group.Key.ToString()), list.ToArray()));
            }
            var result = new Command("Incremental", args.ToArray());
            return result;
        }

        private static Statement SerializeCustomTypes(Dictionary<string, FieldType> types)
        {
            var values = types.Select(
                field => SerializeField(new FieldDefinition(field.Key, field.Value), "Type"));
            var result = new Command("CustomTypes", body: values.ToArray());
            return result;
        }

        private static Command SerializeDeps(StreamDescription[][] dependencyOrder)
        {
            var body = new List<Statement>();
            foreach (var group in dependencyOrder)
            {
                var list = new DataListNode(group.Select(sd => WriteName(sd.ToString())).ToArray());
                body.Add(list);
            }
            var result = new Command("DependencyOrder", body: body.ToArray());
            return result;
        }

        private static Command SerializeStream(StreamDefinition stream)
        {
            var body = new List<Statement>();
            foreach (var field in stream.Fields)
                body.Add(SerializeField(field, "Field"));
            var identity = new Command(IDENTITY.Name, stream.Identity.Select(f => new NameNode(f)).ToArray());
            body.Add(identity);
            if (stream.RareChangeFields?.Length > 0) {
                var rcf = new Command(RCF.Name, stream.RareChangeFields.Select(f => new NameNode(f)).ToArray());
                body.Add(rcf);
            }
            if (stream.DomainReductions?.Length > 0) {
                var pairs = stream.DomainReductions
                    .Select(p => new KeyValuePair<string, Expression>(p.Key, new IntegerNode(p.Value)))
                    .ToArray();
                var drs = new Command(DOMAIN_REDUCTION.Name, new[] { new KvListNode(pairs) });
                body.Add(drs);
            }
            if (stream.SeqIdIndex != -1) {
                var sid = new Command(SEQ_ID.Name, new[] { new IntegerNode(stream.SeqIdIndex) });
                body.Add(sid);
            }
            var result = new Command("Stream", new[] { WriteName(stream.Name.ToString()) }, body: body.ToArray());
            return result;
        }

        private static Statement SerializeField(FieldDefinition field, string typeName)
        {
            var name = new NameNode(field.Name);
            Expression fieldTypeExpr = WriteName(field.Type.Type.ToString());
            if (field.Type.Info != null) {
                fieldTypeExpr = SerializeExtendedType((NameNode)fieldTypeExpr, field.Type.Info);
            }
            var flArgs = new List<Expression>();
            flArgs.Add(fieldTypeExpr);
            if (field.Type.Nullable) {
                flArgs.Add(WriteName("NULL"));
            }
            var fieldList = new NamedListNode(name, flArgs.ToArray());
            var result = new Command(typeName, new[] { fieldList });
            return result;
        }

        private static Expression SerializeExtendedType(NameNode fieldTypeExpr, string info)
        {
            if (info == "-1") {
                return fieldTypeExpr;
            }
            var args = new List<Expression>();
            foreach (var value in info.Split(',')) {
                if (int.TryParse(value, out int num)) {
                    args.Add(new IntegerNode(num));
                }
                else if (value.Contains(' ')) {
                    args.Add(new StringNode(value));
                } else {
                    args.Add(new NameNode(value));
                }
            }
            var result = new NamedListNode(fieldTypeExpr, args.ToArray());
            return result;
        }

        private static Expression WriteName(string v) => v.Contains(' ') ? new StringNode(v) : new NameNode(Capitalize(v));

        private static string Capitalize(string input) =>
            input switch
            {
                null => throw new ArgumentNullException(nameof(input)),
                "" => throw new ArgumentException($"{nameof(input)} cannot be empty", nameof(input)),
                _ => input.First().ToString().ToUpper() + input[1..]
            };

        private static Dictionary<string, DataDictionary> _cache = new();

        public static DataDictionary Parse(string data)
        {
            if (!_cache.TryGetValue(data, out var result)) {
                var module = PansyncData.Parse(data);
                var ast = (Command)module.Body[0];
                result = Parse(ast);
                _cache.Add(data, result);
            }
            return result;
        }

        private static DataDictionary Parse(Command ast)
        {
            if (ast.Name != "DataDictionary" || ast.Body == null) {
                throw new ArgumentException("Not a Data Dictionary file");
            }
            var name = ParseNameExpr(ast.Arguments[0]);
            var streams = ast.Body
                .OfType<Command>()
                .Where(m => m.Name == "Stream")
                .Select(ParseStream)
                .ToArray();
            var deps = ParseDeps(ast.Body.OfType<Command>().Single(m => m.Name == "DependencyOrder"));
            var customTypes = ParseCustomTypes(ast.Body.OfType<Command>().Single(m => m.Name == "CustomTypes"))
                .ToDictionary(f => f.Name, f => f.Type, StringComparer.OrdinalIgnoreCase);
            var incremental = ParseIncrementalData(ast.Body.OfType<Command>().SingleOrDefault(m => m.Name == "Incremental"));
            return new DataDictionary(name, streams, deps, customTypes, incremental);
        }

        private static string ParseNameExpr(Expression expr) =>
            expr switch {
                NameNode refEx => refEx.Name,
                StringNode slEx => slEx.Value,
                _ => expr.ToString()
            };

        private static Dictionary<StreamDescription, IncrementalStrategy> ParseIncrementalData(Command? data)
        {
            var result = new Dictionary<StreamDescription, IncrementalStrategy>();
            if (data?.Arguments.Length > 0) {
                var values = (KvListNode)data.Arguments[0];
                foreach (var pair in values.Values) {
                    var key = Enum.Parse<IncrementalStrategy>(pair.Key);
                    var streams = (NamedListNode)pair.Value;
                    foreach (var item in streams.Arguments) {
                        result.Add(StreamDescription.Parse(item.ToString()), key);
                    }
                }
            }
            return result;
        }

        private static IEnumerable<FieldDefinition> ParseCustomTypes(Command types)
        {
            if (types.Body != null) {
                foreach (var entry in types.Body)
                {
                    yield return ParseField((Command)entry, "Type");
                }
            }
        }

        private static readonly NameNode IDENTITY = new("Identity");
        private static readonly NameNode RCF = new("RarelyChangedFields");
        private static readonly NameNode DOMAIN_REDUCTION = new("DomainReduction");
        private static readonly NameNode SEQ_ID = new("SequentialIndexId");

        private static StreamDefinition ParseStream(Command ast)
        {
            StreamDescription name = StreamDescription.Parse(((NameNode)ast.Arguments[0]).ToString());
            if (ast.Body == null) {
                throw new ArgumentException("Stream body is missing");
            }
            var commands = ast.Body.OfType<Command>().ToArray();
            var fields = commands
                .Where(c => c.Name == "Field")
                .Select(f => ParseField(f, "Field"))
                .ToArray();
            var exprs = commands
                .Where(c => c.Name != "Field")
                .ToArray();
            var idExpr = exprs.SingleOrDefault(e => e.Name == IDENTITY.Name);
            if (idExpr == null) { 
                throw new Exception($"Identity information is missing for stream '{name}.'");
            }
            var identity = idExpr.Arguments.Cast<NameNode>().Select(r => r.Name).ToArray();
            var rcfExpr = exprs.SingleOrDefault(e => e.Name == RCF.Name);
            var rcf = rcfExpr == null ? Array.Empty<string>() : rcfExpr.Arguments.Cast<NameNode>().Select(r => r.Name).ToArray();
            var drExpr = exprs.SingleOrDefault(e => e.Name == DOMAIN_REDUCTION.Name);
            var drs = drExpr == null ? Array.Empty<KeyValuePair<string, long>>() : ParseDomainReduction((KvListNode)drExpr.Arguments[0]);
            var sidExpr = exprs.SingleOrDefault(e => e.Name == SEQ_ID.Name);
            var sid = sidExpr == null ? -1 : (int)((IntegerNode)sidExpr.Arguments[0]).Value;
            return new StreamDefinition(name, fields, identity) { RareChangeFields = rcf, DomainReductions = drs, SeqIdIndex = sid };
        }

        private static FieldDefinition ParseField(Command ast, string typeName)
        {
            if (ast.Name != typeName) {
                throw new ArgumentException($"Invalid field data: {ast.ToString()}");
            }
            var fieldMie = (NamedListNode)ast.Arguments[0];
            var name = fieldMie.Name.ToString();
            var nullable = fieldMie.Arguments.Length == 2;
            var fieldType = ParseType(fieldMie.Arguments[0], nullable);
            return new FieldDefinition(name, fieldType);
        }

        private static FieldType ParseType(Expression typeExpr, bool nullable)
        {
            return typeExpr switch {
                NamedListNode nln =>
                    new FieldType(Enum.Parse<TypeTag>(
                        nln.Name.ToString()), nullable, CollectionType.None, string.Join<Expression>(", ", nln.Arguments)),
                _ => new FieldType(Enum.Parse<TypeTag>(typeExpr.ToString()), nullable, CollectionType.None, null)
            };
        }

        private static StreamDescription[][] ParseDeps(Command deps)
        {
            return deps.Body?.Cast<DataListNode>().Select(ParseDepGroup).ToArray() ?? Array.Empty<StreamDescription[]>();
        }

        private static StreamDescription[] ParseDepGroup(DataListNode list) => list.Values.Select(i => StreamDescription.Parse(i.ToString())).ToArray();

        private static KeyValuePair<string, long>[] ParseDomainReduction(KvListNode bases) =>
            bases.Values
                .Select(p => KeyValuePair.Create(
                    p.Key,
                    ((IntegerNode)p.Value).Value
                ))
                .ToArray();
    }
}
