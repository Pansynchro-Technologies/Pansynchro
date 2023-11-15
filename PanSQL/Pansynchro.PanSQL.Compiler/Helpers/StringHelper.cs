using Microsoft.CodeAnalysis.CSharp;

using Pansynchro.PanSQL.Compiler.DataModels;

namespace Pansynchro.PanSQL.Compiler.Helpers
{
	internal static class StringHelper
	{
		public static string ToLiteral(this string value) => SymbolDisplay.FormatLiteral(value, true);

		public static string ToPropertyName(this string value) => value.Length switch {
			0 => value,
			1 => value.ToUpperInvariant(),
			_ => char.ToUpperInvariant(value[0]) + value[1..]
		};

		public static string ToIndexName(this MemberReferenceExpression field) => field.ToString().Replace(".", "__");
	}
}
