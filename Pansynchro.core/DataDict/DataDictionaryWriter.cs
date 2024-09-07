using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
			var result = new Command("DataDictionary", new[] { WriteName(schema.Name) }, null, body.ToArray());
			return result;
		}

		private static Command SerializeCustomTypes(Dictionary<string, FieldType> types)
		{
			var values = types.Select(
				field => SerializeField(new FieldDefinition(field.Key, field.Value), "Type"));
			var result = new Command("CustomTypes", body: values.ToArray());
			return result;
		}

		private static Command SerializeDeps(StreamDescription[][] dependencyOrder)
		{
			var body = new List<Statement>();
			foreach (var group in dependencyOrder) {
				var list = new DataListNode(group.Select(sd => WriteName(sd.ToString())).ToArray());
				body.Add(list);
			}
			var result = new Command("DependencyOrder", body: body.ToArray());
			return result;
		}

		public static string WriteStream(StreamDefinition stream) => SerializeStream(stream).ToString();

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
			if (stream.SeqIdIndex.HasValue) {
				var sid = new Command(SEQ_ID.Name, new[] { new IntegerNode(stream.SeqIdIndex.Value) });
				body.Add(sid);
			}
			if (stream.CustomQuery != null) {
				var cq = new Command(QUERY.Name, new[] { new StringNode(stream.CustomQuery) });
				body.Add(cq);
			}
			if (stream.AuditFieldIndex.HasValue) {
				var aud = new Command(AUDIT_ID.Name, new[] { new IntegerNode(stream.AuditFieldIndex.Value) });
				body.Add(aud);
			}
			var result = new Command("Stream", new[] { WriteName(stream.Name.ToString()) }, body: body.ToArray());
			return result;
		}

		private static Command SerializeField(FieldDefinition field, string typeName)
		{
			var name = new NameNode(field.Name);
			Expression fieldTypeExpr = WriteName(field.Type.Type.ToString());
			if (field.Type.Info != null) {
				fieldTypeExpr = SerializeExtendedType((NameNode)fieldTypeExpr, field.Type.Info);
			}
			var flArgs = new List<Expression>();
			flArgs.Add(fieldTypeExpr);
			if (field.Type.Nullable) {
				flArgs.Add(NULL_EXPR);
			}
			if (field.Type.Incompressible) {
				flArgs.Add(INCOMPRESSIBLE_EXPR);
			}
			if (field.CustomRead != null) {
				flArgs.Add(new StringNode(field.CustomRead));
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
				} else if (value.Contains(' ')) {
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
			input switch {
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
			return new DataDictionary(name, streams, deps, customTypes);
		}

		private static string ParseNameExpr(Expression expr) =>
			expr switch {
				NameNode refEx => refEx.Name,
				StringNode slEx => slEx.Value,
				_ => expr.ToString()
			};

		private static IEnumerable<FieldDefinition> ParseCustomTypes(Command types)
		{
			if (types.Body != null) {
				foreach (var entry in types.Body) {
					yield return ParseField((Command)entry, "Type");
				}
			}
		}

		private static readonly NameNode IDENTITY = new("Identity");
		private static readonly NameNode RCF = new("RarelyChangedFields");
		private static readonly NameNode DOMAIN_REDUCTION = new("DomainReduction");
		private static readonly NameNode SEQ_ID = new("SequentialIndex");
		private static readonly NameNode AUDIT_ID = new("AuditIndex");
		private static readonly NameNode QUERY = new("CustomQuery");

		public static StreamDefinition ParseStream(string data)
		{
			var module = PansyncData.Parse(data);
			var ast = (Command)module.Body[0];
			return ParseStream(ast);
		}

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
			int? sid = sidExpr == null ? null : (int)((IntegerNode)sidExpr.Arguments[0]).Value;
			var cqExpr = exprs.SingleOrDefault(e => e.Name == QUERY.Name);
			var cq = cqExpr == null ? null : (string)((StringNode)cqExpr.Arguments[0]).Value;
			var audExpr = exprs.SingleOrDefault(e => e.Name == AUDIT_ID.Name);
			int? aud = audExpr == null ? null : (int)((IntegerNode)audExpr.Arguments[0]).Value;
			return new StreamDefinition(name, fields, identity) {
				RareChangeFields = rcf,
				DomainReductions = drs,
				SeqIdIndex = sid,
				AuditFieldIndex = aud,
				CustomQuery = cq
			};
		}

		private static readonly Expression NULL_EXPR = WriteName("NULL");
		private static readonly Expression INCOMPRESSIBLE_EXPR = WriteName("INCOMPRESSIBLE");

		private static FieldDefinition ParseField(Command ast, string typeName)
		{
			if (ast.Name != typeName) {
				throw new ArgumentException($"Invalid field data: {ast.ToString()}");
			}
			var fieldArgs = (NamedListNode)ast.Arguments[0];
			var name = fieldArgs.Name.ToString();
			var nullable = fieldArgs.Arguments.Length >= 2 && fieldArgs.Arguments[1].Matches(NULL_EXPR);
			var incompressible = fieldArgs.Arguments.Length >= 2 && fieldArgs.Arguments.Any(a => a.Matches(INCOMPRESSIBLE_EXPR));
			var hasCustom = fieldArgs.Arguments.Length >= 2 && fieldArgs.Arguments[^1] is StringNode;
			var custom = hasCustom ? fieldArgs.Arguments[^1].ToString() : null;
			var fieldType = ParseType(fieldArgs.Arguments[0], nullable);
			fieldType.Incompressible = incompressible;
			return new FieldDefinition(name, fieldType, custom);
		}

		private static FieldType ParseType(Expression typeExpr, bool nullable)
		{
			return typeExpr switch {
				NamedListNode nln =>
					new FieldType(
						Enum.Parse<TypeTag>(nln.Name.ToString()),
						nullable,
						CollectionType.None,
						string.Join<Expression>(", ", nln.Arguments)),
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
