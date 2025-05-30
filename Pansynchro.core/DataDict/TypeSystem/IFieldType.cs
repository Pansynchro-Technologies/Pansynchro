namespace Pansynchro.Core.DataDict.TypeSystem;
public interface IFieldType
{
	bool Incompressible { get; }
	bool Nullable { get; }
	void Accept(IFieldTypeVisitor visitor);
	T Accept<T>(IFieldTypeVisitor<T> visitor);
	bool CanAssignNotNullToNull(IFieldType other);
	bool CanAssignSpecificToGeneral(IFieldType other) => CanAssignNotNullToNull(other);
	string ToString();
	IFieldType MakeNull();
}
