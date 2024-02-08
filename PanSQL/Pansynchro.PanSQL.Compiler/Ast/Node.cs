using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using Pansynchro.PanSQL.Compiler.Steps;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Ast
{
	public abstract class Node
	{
		internal abstract void Accept(IVisitor visitor);
	}

	public class PanSqlFile(Statement[] lines) : Node
	{
		public Statement[] Lines { get; } = lines;
		public string? Filename { get; internal set; }
		internal Dictionary<string, Variable> Vars { get; } = new(StringComparer.InvariantCultureIgnoreCase);
		internal Dictionary<string, string> Mappings { get; set; } = null!;
		internal NullableDictionary<string, string> NsMappings { get; set; } = null!;
		internal Dictionary<Variable, string> Transformers { get; } = [];
		internal Dictionary<Variable, string> Producers { get; } = [];
		internal List<DataClassModel> Database { get; } = [];
		internal Dictionary<string, TypeDefinition> Types { get; } = [];

		public void AddVar(Variable v, Node n)
		{
			if (Vars.ContainsKey(v.Name)) {
				throw new CompilerError($"Duplicate variable name: '{v.Name}'", n);
			}
			Vars.Add(v.Name, v);
		}

		internal override void Accept(IVisitor visitor) => visitor.OnFile(this);
	}

	public abstract class Statement : Node { }

	public class LoadStatement(string name, string filename) : Statement
	{
		public string Name { get; } = name;
		public string Filename { get; } = filename;

		public DataDictionary Dict { get; set; } = null!;

		internal override void Accept(IVisitor visitor) => visitor.OnLoadStatement(this);
	}

	public class SaveStatement(string name, string filename) : Statement
	{
		public string Name { get; } = name;
		public string Filename { get; } = filename;

		internal override void Accept(IVisitor visitor) => visitor.OnSaveStatement(this);
	}

	public class TypeDefinition(StreamDefinition definition) : Statement
	{
		public StreamDefinition Definition { get; } = definition;

		internal override void Accept(IVisitor visitor) => visitor.OnTypeDefinition(this);
	}

	public enum VarDeclarationType
	{
		Table,
		Stream
	}

	public class VarDeclaration(VarDeclarationType type, string name, CompoundIdentifier identifier) : Statement
	{
		public VarDeclarationType Type { get; } = type;
		public string Name { get; } = name;
		public CompoundIdentifier Identifier { get; } = identifier;

		public StreamDefinition? Stream { get; internal set; }

		internal override void Accept(IVisitor visitor) => visitor.OnVarDeclaration(this);
	}

	public enum OpenType
	{
		Read,
		Write,
		Analyze,
		Source,
		Sink
	}

	public class OpenStatement(string name, string connector, OpenType type, Identifier? dictionary, CredentialExpression creds, Identifier? source) : Statement
	{
		public string Name { get; } = name;
		public string Connector { get; internal set; } = connector;
		public OpenType Type { get; } = type;
		public Identifier? Dictionary { get; } = dictionary;
		public CredentialExpression Creds { get; internal set; } = creds;
		public Identifier? Source { get; } = source;

		internal override void Accept(IVisitor visitor) => visitor.OnOpenStatement(this);
	}

	[Flags]
	internal enum TransactionType
	{
		PureMemory = 0,
		Streamed = 1,
		ToStream = 1 << 1,
		Joined = 1 << 2,
		Grouped = 1 << 3,
		WithCte = 1 << 4,
	}

	public class AnalyzeStatement(Identifier conn, Identifier dict, AnalyzeOption[]? options) : Statement
	{
		public Identifier Conn { get; } = conn;
		public Identifier Dict { get; } = dict;
		public AnalyzeOption[]? Options { get; } = options;

		public bool Optimize => Options?.Any(o => o.Type == AnalyzeOptionType.Optimize) == true;
		public Expression[]? IncludeList => Options?.FirstOrDefault(o => o.Type == AnalyzeOptionType.Include)?.Values;
		public Expression[]? ExcludeList => Options?.FirstOrDefault(o => o.Type == AnalyzeOptionType.Exclude)?.Values;

		internal override void Accept(IVisitor visitor) => visitor.OnAnalyzeStatement(this);
	}

	internal record CteData(string Name, SqlModel Model, StreamDefinition Stream);

	public class SqlTransformStatement(SqlStatement sqlNode, Identifier dest) : Statement
	{
		public SqlStatement SqlNode { get; } = sqlNode;
		public Identifier Dest { get; } = dest;

		internal List<Variable> Tables { get; } = [];
		internal Variable Output { get; set; } = null!;
		internal TransactionType TransactionType { get; set; }
		internal SqlModel DataModel { get; set; } = null!;
		internal IndexData Indices { get; set; } = null!;
		internal List<CteData> Ctes { get; } = [];

		internal override void Accept(IVisitor visitor) => visitor.OnSqlStatement(this);
	}

	public class MapStatement(CompoundIdentifier source, CompoundIdentifier dest, MappingExpression[] mappings, bool isNs) : Statement
	{
		public CompoundIdentifier Source { get; } = source;
		public CompoundIdentifier Dest { get; } = dest;
		public MappingExpression[] Mappings { get; } = mappings;
		public bool IsNS { get; } = isNs;
		public (StreamDefinition s, StreamDefinition d) Streams { get; internal set; }

		internal override void Accept(IVisitor visitor) => visitor.OnMapStatement(this);
	}

	public class SyncStatement(Identifier input, Identifier output) : Statement
	{
		public Identifier Input { get; } = input;
		public Identifier Output { get; } = output;

		internal override void Accept(IVisitor visitor) => visitor.OnSyncStatement(this);
	}

	internal class ScriptVarDeclarationStatement(ScriptVarExpression name, TypeReferenceExpression type, Expression? expr) : Statement
	{
		public ScriptVarExpression Name { get; } = name;
		public TypeReferenceExpression Type { get; } = type;
		public Expression? Expr { get; } = expr;

		internal FieldType FieldType { get; set; } = null!;
		internal Identifier ScriptName { get; set; } = null!;

		internal override void Accept(IVisitor visitor) => visitor.OnScriptVarDeclarationStatement(this);
	}

	public abstract class Expression : Node { }

	public abstract class TypedExpression : Expression
	{
		internal abstract FieldType ExpressionType { get; }
	}

	public class Identifier(string name) : Expression
	{
		public string Name { get; } = name;

		internal override void Accept(IVisitor visitor) => visitor.OnIdentifier(this);

		public override string ToString() => Name;
	}

	public class ScriptVarExpression(string name) : TypedExpression
	{
		public string Name { get; internal set; } = name;

		internal FieldType VarType { get; set; } = null!;

		internal override FieldType ExpressionType => VarType;

		internal override void Accept(IVisitor visitor) => visitor.OnScriptVarExpression(this);

		public override string ToString() => Name;
	}

	public class TypeReferenceExpression(string name, int? magnitude, bool isArray) : Expression
	{
		public string Name { get; } = name;
		public int? Magnitude { get; } = magnitude;
		public bool IsArray { get; } = isArray;

		internal override void Accept(IVisitor visitor)
		{ }
	}

	public class CompoundIdentifier(string? parent, string? name) : Expression
	{
		public string? Name { get; } = name;
		public string? Parent { get; } = parent;

		internal override void Accept(IVisitor visitor) => visitor.OnCompoundIdentifier(this);

		public override string? ToString() => Parent != null ? $"{Parent}.{Name}" : Name != null ? Name : null;
	}

	public class FunctionCallExpression(string method, Expression[] args) : TypedExpression
	{
		public string Method { get; } = method;
		public Expression[] Args { get; } = args;

		internal FieldType? ReturnType { get; set; }

		internal override FieldType ExpressionType => ReturnType ?? throw new CompilerError($"No return type has been bound", this);

		public string? CodeName { get; internal set; }
		public string? Namespace { get; internal set; }
		public bool IsProp {  get; internal set; }

		internal override void Accept(IVisitor visitor) => visitor.OnFunctionCallExpression(this);

		public override string ToString() => IsProp ? CodeName! : $"{CodeName ?? Method}({string.Join<Expression>(", ", Args)})";
	}

	public class CredentialExpression(string method, TypedExpression value) : Expression
	{
		public string Method { get; } = method;
		public TypedExpression Value { get; } = value;

		internal override void Accept(IVisitor visitor) => visitor.OnCredentialExpression(this);

		public override string ToString() => Method switch {
			"__literal" => ((StringLiteralExpression)Value).Value,
			"__direct" => Value.ToString()!,
			_ => $"{Method}({Value})"
		};
	}

	public class MappingExpression(Identifier key, Identifier value) : Expression
	{
		public Identifier Key { get; } = key;
		public Identifier Value { get; } = value;

		internal override void Accept(IVisitor visitor) => visitor.OnMappingExpression(this);
	}

	public enum AnalyzeOptionType
	{
		Optimize,
		Include,
		Exclude
	}

	public class AnalyzeOption(AnalyzeOptionType type, Expression[]? values = null) : Expression
	{
		public AnalyzeOptionType Type { get; } = type;
		public Expression[]? Values { get; } = values;

		internal override void Accept(IVisitor visitor) => visitor.OnAnalyzeOption(this);
	}

	public abstract class LiteralExpression : TypedExpression
	{ }

	public class StringLiteralExpression(string value) : LiteralExpression
	{
		public string Value { get; } = value;

		internal override FieldType ExpressionType => TypesHelper.TextType;

		public override string ToString() => Value.ToLiteral();

		internal override void Accept(IVisitor visitor) => visitor.OnStringLiteralExpression(this);
	}

	public class IntegerLiteralExpression(int value) : LiteralExpression
	{
		public int Value { get; } = value;

		internal override FieldType ExpressionType => TypesHelper.TextType;

		public override string ToString() => Value.ToString();

		internal override void Accept(IVisitor visitor) => visitor.OnIntegerLiteralExpression(this);
	}

	public class JsonLiteralExpression(JsonNode value) : LiteralExpression
	{
		internal static JsonSerializerOptions _options = new JsonSerializerOptions { WriteIndented = false };

		public JsonNode Value { get; } = value;

		internal override FieldType ExpressionType => TypesHelper.JsonType;

		internal override void Accept(IVisitor visitor) => visitor.OnJsonLiteralExpression(this);

		public override string ToString() => Value.ToJsonString(_options).ToLiteral();
	}

	public class JsonInterpolatedExpression(JsonNode value, List<KeyValuePair<JsonIndexing, Expression>> ints) : TypedExpression
	{
		public JsonNode Value { get; } = value;
		public List<KeyValuePair<JsonIndexing, Expression>> Ints { get; } = ints;

		internal string? VarName { get; set; }

		internal override FieldType ExpressionType => TypesHelper.JsonType;
		internal override void Accept(IVisitor visitor) => visitor.OnJsonInterpolatedExpression(this);
		internal string JsonString => Value.ToJsonString(JsonLiteralExpression._options).ToLiteral();
		public override string ToString() => (VarName ?? throw new Exception("Interpolated string has not been processed")) + ".ToJsonString()";
	}

	public class JsonIndexing
	{
		public JsonIndexing? Child { get; }
		public int? ArrIndex { get; }
		public string? ObjIndex { get; }

		internal bool IsInsert => Child?.IsInsert ?? ArrIndex.HasValue;

		private JsonIndexing(JsonIndexing? child, int value)
		{
			Child = child;
			ArrIndex = value;
		}

		private JsonIndexing(JsonIndexing? child, string value)
		{
			Child = child;
			ObjIndex = value;
		}

		public JsonIndexing(int value)
		{
			ArrIndex = value;
		}

		public JsonIndexing(string value)
		{
			ObjIndex = value;
		}

		public JsonIndexing Reparent(int value) => new JsonIndexing(this, value);

		public JsonIndexing Reparent(string value) => new JsonIndexing(this, value);

		public string ToCodeString(string parent, Expression value)
		{
			if (IsInsert) {
				if (Child != null) {
					if (parent.EndsWith(']')) {
						parent = parent + '!';
					}
					parent = parent + (ArrIndex.HasValue ? $"[{ArrIndex}]" : $"[\"{ObjIndex}\"]");
					return Child.ToCodeString(parent, value);
				}
				return $"((JsonArray){parent}!).Insert({ArrIndex}, {value})";
			}
			return $"{parent}{this} = {value}";
		}

		public override string ToString()
		{
			var result = ArrIndex.HasValue ? $"[{ArrIndex}]" : $"[\"{ObjIndex}\"]";
			return Child == null ? result : result + '!' + Child.ToString();
		}
	}
}