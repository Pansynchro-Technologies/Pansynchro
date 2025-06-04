using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.Core.DataDict.TypeSystem;
public record TupleField(string? Name, KeyValuePair<string, IFieldType>[] Fields, bool Nullable) : IFieldType
{
	public bool Incompressible => Fields.Any(f => f.Value.Incompressible);

	public override string ToString()
	{
		var result = $"Tuple[{string.Join(", ", Fields.Select(f => $"{f.Key} {f.Value}"))}]";
		if (Nullable) {
			result = result + " NULL";
		}
		return result;
	}

	public void Accept(IFieldTypeVisitor visitor) => visitor.VisitTupleField(this);
	public T Accept<T>(IFieldTypeVisitor<T> visitor) => visitor.VisitTupleField(this);
	public IFieldType MakeNull() => this.Nullable ? this : this with { Nullable = true };

	public bool CanAssignNotNullToNull(IFieldType other) => other is TupleField tf
		&& tf.Name == Name
		&& tf.Fields.Length == Fields.Length
		&& tf.Fields.Select(f => f.Value).SequenceEqual(Fields.Select(f => f.Value))
		&& (!Nullable)
		&& tf.Nullable;
}
