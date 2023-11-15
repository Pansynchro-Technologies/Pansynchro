using System;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.DataModels;

namespace Pansynchro.PanSQL.Compiler.Helpers
{
	internal class TypesHelper
	{
		internal static FieldType NullType = new(TypeTag.Custom, true, CollectionType.None, "NULL");

		internal static FieldType IntType = new(TypeTag.Int, false, CollectionType.None, null);

		internal static FieldType MakeStringType(string value) => new(TypeTag.Char, false, CollectionType.None, value.Length.ToString());

		internal static DataClassModel BuildDataClass(StreamDefinition stream)
		{
			var fields = stream.Fields.Select(BuildDataField).ToArray();
			var pk = stream.Identity.Select(n => stream.Fields.IndexWhere(f => f.Name == n).First()).ToArray();
			return new(stream.Name.ToString().Replace('.', '_') + '_', fields, pk);
		}

		private static DataFieldModel BuildDataField(FieldDefinition field)
		{
			var type = FieldTypeToCSharpType(field.Type);
			var initializer = FieldTypeToInitializer(type);
			return new(field.Name, type, initializer);
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

		public static string FieldTypeToCSharpType(FieldType type)
		{
			var baseType = type.Type switch {
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
				TypeTag.Xml or TypeTag.Json => "string",
				TypeTag.Money or TypeTag.SmallMoney => "decimal",
				TypeTag.SmallDateTime => "DateTime",
				TypeTag.SByte => "sbyte",
				TypeTag.UShort => "ushort",
				TypeTag.UInt => "uint",
				TypeTag.ULong => "ulong",
				_ => throw new NotImplementedException(),
			};
			if (type.Nullable) {
				baseType += '?';
			}
			var typeName = type.CollectionType switch {
				CollectionType.None => baseType,
				CollectionType.Array => baseType + "[]",
				CollectionType.Multiset => throw new NotImplementedException(),
				_ => throw new NotImplementedException(),
			};
			return typeName;
		}

		internal static string FieldTypeToGetter(FieldType type, int idx)
		{
			var getter = type.Type switch {
				TypeTag.Char => "r.GetChar",
				TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext => "r.GetString",
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
			return key.Length == 1 ? $"{prefix}.{key[0].Name}" : $"({string.Join(", ", key.Select(k => $"{prefix}.{k.Name}"))})";
		}

		private static DataFieldModel[] PrimaryKey(DataClassModel model) => model.PkIndex.Select(i => model.Fields[i]).ToArray();
	}
}
