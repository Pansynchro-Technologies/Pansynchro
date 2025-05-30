namespace Pansynchro.Core.DataDict.TypeSystem;
public record CollectionField(IFieldType BaseType, CollectionType CollectionType, bool Nullable) : IFieldType
{
	public bool Incompressible => BaseType.Incompressible;

	public override string ToString()
	{
		var result = $"{CollectionType}[{BaseType}]";
		if (Nullable) {
			result = result + " NULL";
		}
		return result;
	}

	public void Accept(IFieldTypeVisitor visitor) => visitor.VisitCollection(this);
	public T Accept<T>(IFieldTypeVisitor<T> visitor) => visitor.VisitCollection(this);
	public IFieldType MakeNull() => this.Nullable ? this : this with { Nullable = true };

	public bool CanAssignNotNullToNull(IFieldType other) => other is CollectionField cf
		&& cf.BaseType == BaseType
		&& cf.CollectionType == CollectionType
		&& (!Nullable)
		&& cf.Nullable;
}
