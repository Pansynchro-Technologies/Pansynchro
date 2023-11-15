using System;
using System.Collections.Generic;

using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	record IndexRecord(string Name, bool Unique);

	internal class IndexData(Dictionary<string, IndexRecord> values, Dictionary<string, string> lookups)
	{
		private readonly Dictionary<string, IndexRecord> _indices = values;
		private readonly Dictionary<string, string> _lookups = lookups;

		internal IndexRecord Lookup(MemberReferenceExpression tf)
		{
			if (_indices.TryGetValue(tf.ToIndexName(), out var result)) {
				return result; 
			}
			if (_lookups.TryGetValue(tf.Parent.Name, out var tableName)) {
				return _indices[$"{tableName}__{tf.Name}"];
			}
			throw new ArgumentException($"No index data defined for '{tf}'.");
		}
	}
}
