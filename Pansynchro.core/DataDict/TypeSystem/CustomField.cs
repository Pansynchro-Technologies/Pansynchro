namespace Pansynchro.Core.DataDict.TypeSystem;
public record CustomField(string Name, IFieldType BaseType, bool Nullable) : IFieldType
{
	public bool Incompressible => BaseType.Incompressible;

	public void Accept(IFieldTypeVisitor visitor) => visitor.VisitCustomField(this);
	public T Accept<T>(IFieldTypeVisitor<T> visitor) => visitor.VisitCustomField(this);
	public IFieldType MakeNull() => this.Nullable ? this : this with { Nullable = true };

	public bool CanAssignNotNullToNull(IFieldType other) => other is CustomField cf 
		&& cf.Name == Name
		&& (BaseType.CanAssignNotNullToNull(other) || BaseType.Equals(other))
		&& !Nullable
		&& BaseType.Nullable;
}
