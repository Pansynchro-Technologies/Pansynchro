namespace Pansynchro.Core.DataDict.TypeSystem;
public record BasicField(TypeTag Type, bool Nullable, string? Info, bool Incompressible) : IFieldType
{
	public void Accept(IFieldTypeVisitor visitor) => visitor.VisitBasicField(this);
	public T Accept<T>(IFieldTypeVisitor<T> visitor) => visitor.VisitBasicField(this);
	public IFieldType MakeNull() => this.Nullable ? this : this with { Nullable = true };

	public override string ToString()
	{
		var result = Type.ToString();
		if (Info != null) {
			result += $"({Info})";
		}
		if (Nullable) {
			result += " NULL";
		}
		return result;
	}

	public bool CanAssignNotNullToNull(IFieldType other) => other is BasicField bf
		&& (!Nullable) && bf.Nullable
		&& Type == bf.Type
		&& Info == bf.Info;

	public bool CanAssignSpecificToGeneral(IFieldType other) => other is BasicField bf
		&& ((Nullable == bf.Nullable) || ((!Nullable) && bf.Nullable))
		&& Type == bf.Type
		&& (Info != null && bf.Info == null);
}
