using System;
using System.Collections.Generic;
using System.Linq;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Functions
{
	internal struct SpecialFunc(string name, Func<FieldType[], string?> verifyArgs, Func<FieldType[], FieldType> returnType, Func<DbExpression[], Func<DbExpression, string>?, string> codegen)
	{
		public string Name { get; } = name;
		public Func<FieldType[], string?> VerifyArgs { get; } = verifyArgs;
		public Func<FieldType[], FieldType> ReturnType { get; } = returnType;
		public Func<DbExpression[], Func<DbExpression, string>?, string> Codegen { get; } = codegen;
	}

	internal static class SpecialFunctions
	{
		public static IEnumerable<SpecialFunc> ListSpecialFunctions()
		{
			return [AbsFunction(), AcosFunction(), AsinFunction(), AtanFunction(), Atan2Function(), CeilFunction(),
					CosFunction(), DegFunction(), RadFunction(), ExpFunction(), FloorFunction(), LogFunction(),
					Log10Function(), PowFunction(), RandFunction(), RoundFunction(), SignFunction(),
					SinFunction(), SqrtFunction(), SquareFunction(), TanFunction()];
		}

		private static SpecialFunc AbsFunction() => new("ABS", AnyNumeric(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return $"Math.Abs({arg})";
			});

		private static SpecialFunc AcosFunction() => new("ACOS", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Acos({arg})" : $"Math.Acos({arg})";
			});

		private static SpecialFunc AsinFunction() => new("ASIN", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Asin({arg})" : $"Math.Asin({arg})";
			});

		private static SpecialFunc AtanFunction() => new("ATAN", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Atan({arg})" : $"Math.Atan({arg})";
			});

		private static SpecialFunc Atan2Function() => new("ATAN2", AnyFloat(2), IdentityTypeFirst,
			(exprs, reader) => {
				var arg1 = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				var arg2 = reader?.Invoke(exprs[1]) ?? exprs[1].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Atan2({arg1}, {arg2})" : $"Math.Atan2({arg1}, {arg2})";
			});

		private static SpecialFunc CeilFunction() => new("CEILING", AnyFloatOrDecimal(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Ceiling({arg})" : $"Math.Ceiling({arg})";
			});

		private static SpecialFunc CosFunction() => new("COS", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Cos({arg})" : $"Math.Cos({arg})";
			});

		private static SpecialFunc DegFunction() => new("DEGREES", AnyNumeric(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return $"SqlFunctions.Degrees({arg})";
			});

		private static SpecialFunc RadFunction() => new("RADIANS", AnyNumeric(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return $"SqlFunctions.Radians({arg})";
			});

		private static SpecialFunc ExpFunction() => new("EXP", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Exp({arg})" : $"Math.Exp({arg})";
			});

		private static SpecialFunc FloorFunction() => new("Floor", AnyFloatOrDecimal(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Floor({arg})" : $"Math.Floor({arg})";
			});

		private static SpecialFunc LogFunction() => new("LOG", AnyFloat(1, 2), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				if (exprs.Length == 2) {
					var arg2 = reader?.Invoke(exprs[1]) ?? exprs[1].ToString();
					return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Log({arg}, {arg2})" : $"Math.Log({arg}, {arg2})";
				}
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Log({arg})" : $"Math.Log({arg})";
			});

		private static SpecialFunc Log10Function() => new("LOG10", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Log10({arg})" : $"Math.Log10({arg})";
			});

		private static SpecialFunc PowFunction() => new("POWER", AnyFloat(2), IdentityTypeFirst,
			(exprs, reader) => {
				var arg1 = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				var arg2 = reader?.Invoke(exprs[1]) ?? exprs[1].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Pow({arg1}, {arg2})" : $"Math.Pow({arg1}, {arg2})";
			});

		private static SpecialFunc RandFunction() => new("RAND", IntCount(0, 1), _ => TypesHelper.FloatType,
			(exprs, reader) => {
				if (exprs.Length == 1) {
					var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
					return $"SqlFunctions.Rand({arg})";
				}
				return "SqlFunctions.Rand()";
			});

		private static SpecialFunc RoundFunction() => new("ROUND", AnyFloatOrDecimal(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Round({arg})" : $"Math.Round({arg})";
			});

		private static SpecialFunc SignFunction() => new("SIGN", AnyNumeric(1), _ => TypesHelper.IntType,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return $"Math.Sign({arg})";
			});

		private static SpecialFunc SinFunction() => new("SIN", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Sin({arg})" : $"Math.Sin({arg})";
			});

		private static SpecialFunc SqrtFunction() => new("SQRT", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Sqrt({arg})" : $"Math.Sqrt({arg})";
			});

		private static SpecialFunc SquareFunction() => new("SQUARE", AnyNumeric(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return $"SqlFunctions.Square({arg})";
			});

		private static SpecialFunc TanFunction() => new("TAN", AnyFloat(1), IdentityTypeFirst,
			(exprs, reader) => {
				var arg = reader?.Invoke(exprs[0]) ?? exprs[0].ToString();
				return exprs[0].Type!.Type == TypeTag.Float ? $"MathF.Tan({arg})" : $"Math.Tan({arg})";
			});

		private static string? NoArgs(FieldType[] types) 
			=> (types.Length != 0) ? $"Function {{0}} does not take any argument(s)." : null;

		private static Func<FieldType[], string?> AnyNumeric(int count) => types => {
			if (types.Length != count) {
				return $"Function {{0}} requires {count} argument(s).";
			}
			for (int i = 0; i < count; i++) {
				var typ = types[i];
				if ((typ.CollectionType != CollectionType.None) || (!(typ.Type is TypeTag.Int or TypeTag.UInt or TypeTag.Long or TypeTag.ULong or TypeTag.Byte or TypeTag.SByte or TypeTag.Short or TypeTag.UShort or TypeTag.Decimal or TypeTag.Float or TypeTag.Double or TypeTag.Numeric))) {
					return $"'{typ}' is not a numeric type.";
				}
			}
			if (count > 1 && types.Distinct().Count() > 1) {
				return $"All arguments for function {{0}} must be of the same type.";
			}
			return null;
		};

		private static Func<FieldType[], string?> AnyFloat(int count) => types => {
			if (types.Length != count) {
				return $"Function {{0}} requires {count} argument(s).";
			}
			for (int i = 0; i < count; i++) {
				var typ = types[i];
				if ((typ.CollectionType != CollectionType.None) || (!(typ.Type is TypeTag.Float or TypeTag.Double))) {
					return $"'{typ}' is not a float type.";
				}
			}
			if (count > 1 && types.Distinct().Count() > 1) {
				return $"All arguments for function {{0}} must be of the same type.";
			}
			return null;
		};

		private static Func<FieldType[], string?> IntCount(int countMin, int countMax) => types => {
			if (types.Length < countMin || types.Length > countMax) {
				return $"Function {{0}} requires between {countMin} and {countMax} arguments.";
			}
			for (int i = 0; i < types.Length; i++) {
				var typ = types[i];
				if ((typ.CollectionType != CollectionType.None) || (!(typ.Type is TypeTag.Int or TypeTag.UInt or TypeTag.Long or TypeTag.ULong or TypeTag.Byte or TypeTag.SByte or TypeTag.Short or TypeTag.UShort))) {
					return $"'{typ}' is not a float type.";
				}
			}
			if (types.Distinct().Count() > 1) {
				return $"All arguments for function {{0}} must be of the same type.";
			}
			return null;
		};

		private static Func<FieldType[], string?> AnyFloat(int countMin, int countMax) => types => {
			if (types.Length < countMin || types.Length > countMax) {
				return $"Function {{0}} requires between {countMin} and {countMax} arguments.";
			}
			for (int i = 0; i < types.Length; i++) {
				var typ = types[i];
				if ((typ.CollectionType != CollectionType.None) || (!(typ.Type is TypeTag.Float or TypeTag.Double))) {
					return $"'{typ}' is not a float type.";
				}
			}
			if (types.Distinct().Count() > 1) {
				return $"All arguments for function {{0}} must be of the same type.";
			}
			return null;
		};

		private static Func<FieldType[], string?> AnyFloatOrDecimal(int count) => types => {
			if (types.Length != count) {
				return $"Function {{0}} requires {count} argument(s).";
			}
			for (int i = 0; i < count; i++) {
				var typ = types[i];
				if ((typ.CollectionType != CollectionType.None) || (!(typ.Type is TypeTag.Float or TypeTag.Double or TypeTag.Decimal or TypeTag.Numeric))) {
					return $"'{typ}' is not a float or decimal type.";
				}
			}
			if (count > 1 && types.Distinct().Count() > 1) {
				return $"All arguments for function {{0}} must be of the same type.";
			}
			return null;
		};

		private static FieldType IdentityTypeFirst(FieldType[] types) => types[0];
	}
}
