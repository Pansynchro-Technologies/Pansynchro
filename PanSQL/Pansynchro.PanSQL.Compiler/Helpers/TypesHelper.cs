using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Functions;

namespace Pansynchro.PanSQL.Compiler.Helpers
{
	internal static class TypesHelper
	{
		internal static BasicField NullType = new(TypeTag.None, true, null, false);
		internal static BasicField ObjType = new(TypeTag.Unstructured, false, null, false);

		internal static BasicField BoolType = new(TypeTag.Boolean, false, null, false);
		internal static BasicField IntType = new(TypeTag.Int, false, null, false);
		internal static BasicField FloatType = new(TypeTag.Float, false, null, false);
		internal static BasicField DoubleType = new(TypeTag.Double, false, null, false);
		internal static BasicField TextType = new(TypeTag.Ntext, false, null, false);
		internal static BasicField DateTimeType = new(TypeTag.DateTime, false, null, false);
		internal static BasicField JsonType = new(TypeTag.Json, false, null, false);
		internal static BasicField NvarcharType = new(TypeTag.Nvarchar, false, null, false);

		internal static BasicField MakeStringType(string value) => new(TypeTag.Nchar, false, value.Length.ToString(), false);

		internal static DataClassModel BuildDataClass(TypeDefinition stream)
		{
			var result = BuildDataClass(stream.Definition, true);
			result.FieldConstructor = true;
			return result;
		}

		internal static DataClassModel BuildDataClass(StreamDefinition stream, bool fieldConstructor = false)
		{
			var fields = stream.Fields.Select(f => BuildDataField(f, fieldConstructor)).ToArray();
			var pk = stream.Identity.Select(n => stream.Fields.IndexWhere(f => f.Name == n).First()).ToArray();
			return new(stream.Name.ToTableName() + '_', fields, pk) { FieldConstructor = fieldConstructor };
		}

		private static DataFieldModel BuildDataField(FieldDefinition field, bool fieldConstructor)
		{
			var type = FieldTypeToCSharpType(field.Type);
			var initializer = fieldConstructor ? field.Name.ToLower() + '_' : FieldTypeToInitializer(type);
			return new(field.Name.ToPropertyName(), type, initializer);
		}

		private static string FieldTypeToInitializer(string type)
		{
			var isNullable = type.EndsWith('?');
			if (isNullable) {
				type = type[..^1];
			}
			var getter = type switch {
				"string" => "r.GetString({0})",
				"bool" => "r.GetBoolean({0})",
				"byte" => "r.GetByte({0})",
				"short" => "r.GetInt16({0})",
				"int" => "r.GetInt32({0})",
				"long" => "r.GetInt64({0})",
				"decimal" => "r.GetDecimal({0})",
				"float" => "r.GetFloat({0})",
				"double" => "r.GetDouble({0})",
				"DateTime" => "r.GetDateTime({0})",
				"Guid" => "r.GetGuid({0})",
				_ => $"({type})r.GetValue({0})"
			};
			return isNullable ? $"r.IsDBNull({{0}}) ? null : {getter}" : getter;
		}

		private struct TypePrinter : IFieldTypeVisitor<string>
		{
			public string Visit(IFieldType type)
			{
				var result = type.Accept(this);
				if (type.Nullable) {
					result += '?';
				}
				return result;
			}

			public string VisitBasicField(BasicField type) => type.Type switch {
				TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext => "string",
				TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob => "byte[]",
				TypeTag.Boolean => "bool",
				TypeTag.Byte => "byte",
				TypeTag.Short => "short",
				TypeTag.Int => "int",
				TypeTag.Long => "long",
				TypeTag.Decimal or TypeTag.Numeric => "decimal",
				TypeTag.Float or TypeTag.Single => "float",
				TypeTag.Double => "double",
				TypeTag.Date or TypeTag.DateTime => "DateTime",
				TypeTag.DateTimeTZ => "DateTimeOffset",
				TypeTag.Guid => "Guid",
				TypeTag.Xml => "string",
				TypeTag.Json => "System.Text.Json.Nodes.JsonNode",
				TypeTag.Money or TypeTag.SmallMoney => "decimal",
				TypeTag.SmallDateTime => "DateTime",
				TypeTag.SByte => "sbyte",
				TypeTag.UShort => "ushort",
				TypeTag.UInt => "uint",
				TypeTag.ULong => "ulong",
				_ => throw new NotImplementedException(),
			};

			public string VisitCollection(CollectionField type)
			{
				var baseType = Visit(type.BaseType);
				return type.CollectionType switch {
					CollectionType.Array => baseType + "[]",
					CollectionType.Multiset => throw new NotImplementedException(),
					_ => throw new NotImplementedException(),
				};
			}

			public string VisitCustomField(CustomField type)
			{
				throw new NotImplementedException();
			}

			public string VisitTupleField(TupleField type)
			{
				throw new NotImplementedException();
			}
		}

		public static string FieldTypeToCSharpType(IFieldType type) => new TypePrinter().Visit(type);

		private struct DotNetTyper : IFieldTypeVisitor<Type>
		{
			public Type Visit(IFieldType type)
			{
				var result = type.Accept(this);
				if (type.Nullable && result.IsValueType) {
					result = typeof(Nullable<>).MakeGenericType(result);
				}
				return result;
			}

			public Type VisitBasicField(BasicField type) => type.Type switch {
				TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext => typeof(string),
				TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob => typeof(byte[]),
				TypeTag.Boolean => typeof(bool),
				TypeTag.Byte => typeof(byte),
				TypeTag.Short => typeof(short),
				TypeTag.Int => typeof(int),
				TypeTag.Long => typeof(long),
				TypeTag.Decimal or TypeTag.Numeric => typeof(decimal),
				TypeTag.Float or TypeTag.Single => typeof(float),
				TypeTag.Double => typeof(double),
				TypeTag.Date or TypeTag.DateTime => typeof(DateTime),
				TypeTag.DateTimeTZ => typeof(DateTimeOffset),
				TypeTag.Guid => typeof(Guid),
				TypeTag.Xml or TypeTag.Json => typeof(string),
				TypeTag.Money or TypeTag.SmallMoney => typeof(decimal),
				TypeTag.SmallDateTime => typeof(DateTime),
				TypeTag.SByte => typeof(sbyte),
				TypeTag.UShort => typeof(ushort),
				TypeTag.UInt => typeof(uint),
				TypeTag.ULong => typeof(ulong),
				_ => throw new NotImplementedException(),
			};

			public Type VisitCollection(CollectionField type)
			{
				var result = Visit(type.BaseType);
				return type.CollectionType switch {
					CollectionType.Array => result.MakeArrayType(),
					_ => throw new NotImplementedException(),
				};
			}

			public Type VisitCustomField(CustomField type)
			{
				throw new NotImplementedException();
			}

			public Type VisitTupleField(TupleField type)
			{
				throw new NotImplementedException();
			}
		}

		internal static Type FieldTypeToDotNetType(IFieldType type) => new DotNetTyper().Visit(type);

		internal static string FieldTypeToGetter(IFieldType type, int idx)
		{
			var typ = type as BasicField ?? throw new NotImplementedException();
			var getter = typ.Type switch {
				TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext => "r.GetString",
				TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob => "r.GetBytes",
				TypeTag.Boolean => "r.GetBoolean",
				TypeTag.Byte => "r.GetByte",
				TypeTag.Short => "r.GetInt16",
				TypeTag.Int => "r.GetInt32",
				TypeTag.Long => "r.GetInt64",
				TypeTag.Decimal or TypeTag.Numeric => "r.GetDecimal",
				TypeTag.Float or TypeTag.Single => "r.GetFloat",
				TypeTag.Double => "r.GetDouble",
				TypeTag.DateTime or TypeTag.SmallDateTime => "r.GetDateTime",
				TypeTag.DateTimeTZ => "(DateTimeOffset)r.GetValue",
				TypeTag.Guid => "r.GetGuid",
				TypeTag.Xml => "r.GetString",
				TypeTag.Money or TypeTag.SmallMoney => "r.GetDecimal",
				_ => throw new NotImplementedException(),
			};
			getter = $"{getter}({idx})";
			return type.Nullable ? $"(r.IsDBNull({idx}) ? System.DBNull.Value : {getter})" : getter;
		}

		internal static IFieldType CSharpTypeToFieldType(Type returnType)
		{
			if (returnType == typeof(string)) {
				return TextType;
			}
			if (returnType == typeof(int)) {
				return IntType;
			}
			if (returnType == typeof(DateTime)) {
				return DateTimeType;
			}
			if (returnType == typeof(object)) {
				return ObjType;
			}
			if (returnType == typeof(JsonNode)) {
				return JsonType;
			}
			throw new NotImplementedException();
		}

		internal static IFieldType GetFieldType(this TypeReferenceExpression type)
		{
			if (!Enum.TryParse<TypeTag>(type.Name, true, out var tag)) {
				throw new CompilerError($"'{type.Name}' is not a valid variable type name.", type);
			}
			var result = new BasicField(tag, false, type.Magnitude?.ToString(), false);
			return type.IsArray ? new CollectionField(result, CollectionType.Array, false) : result;
		}

		internal static string ModelIdentityType(DataClassModel model)
		{
			var key = PrimaryKey(model);
			return key.Length == 1 ? key[0].Type : $"({string.Join(", ", key.Select(k => k.Type))})";
		}

		internal static string ModelIdentityName(DataClassModel model)
		{
			var key = PrimaryKey(model);
			return '_' + string.Join('_', key.Select(k => k.Name));
		}

		internal static string ModelIdentityFields(DataClassModel model, string prefix)
		{
			var key = PrimaryKey(model);
			return key.Length == 1 ? $"{prefix}.{key[0].Name}" : $"ValueTuple.Create({string.Join(", ", key.Select(k => $"{prefix}.{k.Name}"))})";
		}

		private static DataFieldModel[] PrimaryKey(DataClassModel model) => model.PkIndex.Select(i => model.Fields[i]).ToArray();

		internal static StreamDefinition BuildStreamDefFromDataModel(DataModel model, string name)
		{
			var tables = model.Inputs;
			var fields = model.Outputs.Select(f => BuildFieldDefFromDataModel(f, tables)).ToArray();
			return new StreamDefinition(new(null, name), fields, (model.GroupKey?.Select(mre => mre.Name) ?? fields.Select(f => f.Name)).ToArray());
		}

		private static FieldDefinition BuildFieldDefFromDataModel(DbExpression expr, TableReference[] tables)
		{
			return expr switch {
				AliasedExpression ae => BuildFieldDefFromDataModel(ae.Expr, tables) with { Name = ae.Alias.ToPropertyName()},
				AggregateExpression agg => BuildFieldDefFromDataModel(agg.Args[0], tables),
				CallExpression call => BuildFieldDefFromCall(call, tables),
				MemberReferenceExpression mre => tables
					.FirstOrDefault(t => t.Name.Equals(mre.Parent.Name, StringComparison.InvariantCultureIgnoreCase))
						?.Stream.Fields.FirstOrDefault(f => f.Name.Equals(mre.Name, StringComparison.InvariantCultureIgnoreCase))
					?? throw new Exception($"No field named '{mre}' is available"),
				_ => throw new NotImplementedException()
			};
		}

		private static FieldDefinition BuildFieldDefFromCall(CallExpression call, TableReference[] tables)
			=> tables.SelectMany(t => t.Stream.Fields)
				.FirstOrDefault(f => f.Name.Equals(call.Function.Name, StringComparison.OrdinalIgnoreCase))
			?? BuildDefaultFieldDefFromCall(call);

		private static FieldDefinition BuildDefaultFieldDefFromCall(CallExpression call)
			=> new FieldDefinition(call.Function.Name, FunctionBinder.GetCallType(call));

		internal static KeyValuePair<string, FieldDefinition> LookupField(TableReference[] tables, string fieldName)
		{
			var matches = tables
				.SelectMany(t => t.Stream.Fields, (tr, f) => KeyValuePair.Create(tr.Name, f))
				.Where(f => f.Value.Name.Equals(fieldName, StringComparison.InvariantCultureIgnoreCase))
				.ToArray();
			return matches.Length switch {
				0 => throw new Exception($"No field named {fieldName} is available"),
				1 => matches[0],
				_ => throw new Exception($"Ambiguous field name: {fieldName}. {matches.Length} different tables contain a field by that name.  Make sure to qualify the name.")
			};

		}
	}
}
