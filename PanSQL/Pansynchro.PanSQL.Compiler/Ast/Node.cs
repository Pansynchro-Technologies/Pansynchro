using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using Pansynchro.PanSQL.Compiler.Steps;

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
		internal Dictionary<string, Variable> Vars { get; } = [];
		internal Dictionary<string, string> Mappings { get; set; } = null!;
		internal Dictionary<Variable, string> Transformers { get; } = [];
		internal Dictionary<Variable, string> Producers { get; } = [];
		internal List<DataClassModel> Database { get; } = [];

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

		public StreamDefinition Stream { get; internal set; } = null!;

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

	public class SqlTransformStatement(SqlStatement sqlNode, Identifier dest) : Statement
	{
		public SqlStatement SqlNode { get; } = sqlNode;
		public Identifier Dest { get; } = dest;

		internal List<Variable> Tables { get; } = [];
		internal Variable Output { get; set; } = null!;
		internal TransactionType TransactionType { get; set; }
		internal SqlModel DataModel { get; set; } = null!;
		internal IndexData Indices { get; set; } = null!;

		internal override void Accept(IVisitor visitor) => visitor.OnSqlStatement(this);
	}

	public class MapStatement(CompoundIdentifier source, CompoundIdentifier dest, MappingExpression[] mappings) : Statement
	{
		public CompoundIdentifier Source { get; } = source;
		public CompoundIdentifier Dest { get; } = dest;
		public MappingExpression[] Mappings { get; } = mappings;
		public (StreamDefinition s, StreamDefinition d) Streams { get; internal set; }

		internal override void Accept(IVisitor visitor) => visitor.OnMapStatement(this);
	}

	public class SyncStatement(Identifier input, Identifier output) : Statement
	{
		public Identifier Input { get; } = input;
		public Identifier Output { get; } = output;

		internal override void Accept(IVisitor visitor) => visitor.OnSyncStatement(this);
	}

	public abstract class Expression : Node { }

	public class Identifier(string name) : Expression
	{
		public string Name { get; } = name;

		internal override void Accept(IVisitor visitor) => visitor.OnIdentifier(this);

		public override string ToString() => Name;
	}

	public class CompoundIdentifier(string parent, string name) : Expression
	{
		public string Name { get; } = name;
		public string Parent { get; } = parent;

		internal override void Accept(IVisitor visitor) => visitor.OnCompoundIdentifier(this);

		public override string ToString() => $"{Parent}.{Name}";
	}

	public class CredentialExpression(string method, string value) : Expression
	{
		public string Method { get; } = method;
		public string Value { get; } = value;

		internal override void Accept(IVisitor visitor) => visitor.OnCredentialExpression(this);

		public override string ToString() => Method switch {
			"__literal" => Value,
			"__direct" => Value.ToLiteral(),
			_ => $"{Method}({Value.ToLiteral()})"
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
}
