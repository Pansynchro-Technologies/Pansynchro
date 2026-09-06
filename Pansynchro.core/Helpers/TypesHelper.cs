using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Core.Helpers;

public static class TypesHelper
{
	public static Type TypeTagToDotNetType(TypeTag type) => type switch {
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
		TypeTag.Date => typeof(DateOnly),
		TypeTag.DateTime => typeof(DateTime),
		TypeTag.DateTimeTZ => typeof(DateTimeOffset),
		TypeTag.Interval => typeof(TimeSpan),
		TypeTag.Guid => typeof(Guid),
		TypeTag.Xml => typeof(string),
		TypeTag.Json => typeof(JsonNode),
		TypeTag.Money or TypeTag.SmallMoney => typeof(decimal),
		TypeTag.SmallDateTime => typeof(DateTime),
		TypeTag.SByte => typeof(sbyte),
		TypeTag.UShort => typeof(ushort),
		TypeTag.UInt => typeof(uint),
		TypeTag.ULong => typeof(ulong),
		_ => throw new NotImplementedException(),
	};

	private readonly struct DotNetTyper : IFieldTypeVisitor<Type>
	{
		public readonly Type Visit(IFieldType type)
		{
			var result = type.Accept(this);
			if (type.Nullable && result.IsValueType) {
				result = typeof(Nullable<>).MakeGenericType(result);
			}
			return result;
		}

		public readonly Type VisitBasicField(BasicField type) => TypeTagToDotNetType(type.Type);

		public readonly Type VisitCollection(CollectionField type)
		{
			var result = Visit(type.BaseType);
			return type.CollectionType switch {
				CollectionType.Array => result.MakeArrayType(),
				_ => throw new NotImplementedException(),
			};
		}

		public readonly Type VisitCustomField(CustomField type)
		{
			throw new NotImplementedException();
		}

		public readonly Type VisitTupleField(TupleField type)
		{
			throw new NotImplementedException();
		}
	}

	public static Type FieldTypeToDotNetType(IFieldType type) => new DotNetTyper().Visit(type);

	public static string TypeTagToCSharpType(TypeTag type) => type switch {
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
		TypeTag.Date => "DateOnly",
		TypeTag.DateTime => "DateTime",
		TypeTag.DateTimeTZ => "DateTimeOffset",
		TypeTag.Interval => "TimeSpan",
		TypeTag.Guid => "Guid",
		TypeTag.Json => "JsonNode",
		TypeTag.Xml => "string",
		TypeTag.Money or TypeTag.SmallMoney => "decimal",
		TypeTag.SmallDateTime => "DateTime",
		TypeTag.SByte => "sbyte",
		TypeTag.UShort => "ushort",
		TypeTag.UInt => "uint",
		TypeTag.ULong => "ulong",
		TypeTag.None => "object",
		_ => throw new NotImplementedException(),
	};

	private readonly struct TypePrinter : IFieldTypeVisitor<string>
	{
		public readonly string Visit(IFieldType type)
		{
			var result = type.Accept(this);
			if (type.Nullable) {
				result += '?';
			}
			return result;
		}

		public readonly string VisitBasicField(BasicField type) => TypeTagToCSharpType(type.Type);

		public readonly string VisitCollection(CollectionField type)
		{
			var baseType = Visit(type.BaseType);
			return type.CollectionType switch {
				CollectionType.Array => $"{baseType}[]",
				_ => throw new NotImplementedException(),
			};
		}

		public readonly string VisitCustomField(CustomField type)
		{
			throw new NotImplementedException();
		}

		public readonly string VisitTupleField(TupleField type) => type.Name ?? "";
	}

	public static string FieldTypeToCSharpType(IFieldType type)
		=> new TypePrinter().Visit(type);
}
