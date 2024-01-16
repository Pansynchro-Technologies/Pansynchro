using System.Collections.Generic;
using System.Text;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal abstract class CSharpStatement
	{
		protected CSharpStatement() { }

		public abstract void Serialize(StringBuilder sb, int indent);

		public static implicit operator CSharpStatement(DbExpression expr) => new ExpressionStatement(expr);

		public string Serialize(int depth)
		{
			var sb = new StringBuilder();
			Serialize(sb, depth);
			return sb.ToString();
		}

		public string Serialize() => Serialize(0);
	}

	class FileModel(ImportModel?[] imports, ClassModel[] classes) : CSharpStatement
	{
		private readonly ImportModel?[] _imports = imports;
		private readonly ClassModel[] _classes = classes;

		public override void Serialize(StringBuilder sb, int indent)
		{
			foreach (var imp in _imports) {
				if (imp == null) {
					sb.AppendLine();
				} else {
					imp.Serialize(sb, indent);
				}
			}
			var multiple = false;
			foreach(var cls in _classes) {
				if (multiple) {
					sb.AppendLine();
				}
				cls.Serialize(sb, indent);
				multiple = true;
			}
		}
	}

	class ImportModel(string name, bool isStatic = false) : CSharpStatement
	{
		public string Name { get; } = name;
		private readonly bool _isStatic = isStatic;

		public static implicit operator ImportModel(string value) => new(value);

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append(_isStatic ? "using static " : "using ");
			sb.Append(Name);
			sb.AppendLine(";");
		}
	}

	class ClassModel(string visibility, string name, string? baseClass, ClassModel[]? subclasses, DataFieldModel[] fields, Method[] methods) : CSharpStatement
	{
		private readonly string _visibility = visibility;
		private readonly string _name = name;
		private readonly string? _baseClass = baseClass;
		private readonly ClassModel[]? _subclasses = subclasses;
		private readonly DataFieldModel[] _fields = fields;
		private readonly Method[] _methods = methods;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append(string.IsNullOrWhiteSpace(_visibility) ? $"class {_name}" : $"{_visibility} class {_name}");
			if (_baseClass != null) {
				sb.Append($" : {_baseClass}");
			}
			sb.AppendLine(" {");
			if (_subclasses != null) {
				foreach  (var sub in _subclasses) {
					sub.Serialize(sb, indent + 1);
					sb.AppendLine();
				}
			}
			foreach (var field in _fields) {
				SerializeField(sb, indent + 1, field);
			}
			var multiple = _fields.Length > 0 || _subclasses?.Length > 0;
			foreach (var method in _methods) {
				if (multiple) {
					sb.AppendLine();
				}
				method.Serialize(sb, indent + 1);
				multiple = true;
			}
			sb.Append('\t', indent);
			sb.AppendLine("}");
		}

		private static void SerializeField(StringBuilder sb, int indent, DataFieldModel field)
		{
			sb.Append('\t', indent);
			sb.Append(field.IsProp ? "public " : "private ");
			if (field.IsReadonly) {
				sb.Append("readonly ");
			}
			sb.Append($"{field.Type} {field.Name}");
			if (field.IsProp) {
				sb.Append(" { get; }");
			}
			if (field.Initializer != null) {
				sb.Append($" = {field.Initializer};");
			} else if (!field.IsProp) {
				sb.Append(';');
			}
			sb.AppendLine();
		}
	}

	class Method(string visibility, string name, string type, string? args, Block body, bool isCtor = false, string? ctorBaseArgs = null) : CSharpStatement
	{
		private readonly string _visibility = visibility;
		public string Name { get; } = name;
		private readonly string _type = type;
		private readonly string? _args = args;
		private readonly Block _body = body;
		private readonly bool _isCtor = isCtor;
		private readonly string? _ctorBaseArgs = ctorBaseArgs;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append(_visibility);
			sb.Append(' ');
			if (!_isCtor) {
				sb.Append(_type);
				sb.Append(' ');
			}
			sb.Append($"{Name}({_args})");
			if (_ctorBaseArgs != null) {
				sb.Append($" : base({_ctorBaseArgs})");
			}
			_body.Serialize(sb, indent);
		}
	}

	class Block(CSharpStatement[] lines) : CSharpStatement
	{
		private readonly CSharpStatement[] _lines = lines;

		public static implicit operator Block(List<CSharpStatement> lines) => new([.. lines]);

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.AppendLine(" {");
			foreach (var line in _lines) {
				line.Serialize(sb, indent + 1);
			}
			sb.Append('\t', indent);
			sb.AppendLine("}");
		}
	}

	class IfStatement(DbExpression cond, Block body) : CSharpStatement
	{
		private readonly DbExpression _cond = cond;
		private readonly Block _body = body;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append($"if ({_cond})");
			_body.Serialize(sb, indent);
		}
	}

	class WhileLoop(DbExpression cond, Block body) : CSharpStatement
	{
		private readonly DbExpression _cond = cond;
		private readonly Block _body = body;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append($"while ({_cond})");
			_body.Serialize(sb, indent);
		}
	}

	class ForeachLoop(string variable, string collection, Block body) : CSharpStatement
	{
		private readonly string _variable = variable;
		private readonly string _collection = collection;
		private readonly Block _body = body;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append($"foreach (var {_variable} in {_collection})");
			_body.Serialize(sb, indent);
		}
	}

	class ExpressionStatement(DbExpression expr) : CSharpStatement
	{
		private readonly DbExpression _expr = expr;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.Append(_expr.ToString());
			sb.AppendLine(";");
		}
	}

	class YieldReturn(DbExpression expr) : CSharpStatement
	{
		private readonly DbExpression _expr = expr;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.AppendLine($"yield return {_expr};");
		}
	}

	class YieldBreak : CSharpStatement
	{
		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.AppendLine($"yield break;");
		}
	}

	class ReturnStatement (DbExpression? value = null) : CSharpStatement
	{
		private readonly DbExpression? _value = value;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.AppendLine(_value != null ? $"return {_value};" : "return;");
		}
	}

	class VarDecl(string name, DbExpression value) : CSharpStatement
	{ 
		private readonly string _name = name;
		private readonly DbExpression _value = value;

		public override void Serialize(StringBuilder sb, int indent)
		{
			sb.Append('\t', indent);
			sb.AppendLine($"var {_name} = {_value};");
		}
	}
}
