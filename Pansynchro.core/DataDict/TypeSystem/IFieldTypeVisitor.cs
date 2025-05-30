namespace Pansynchro.Core.DataDict.TypeSystem;

public interface IFieldTypeVisitor
{
	void VisitBasicField(BasicField type);
	void VisitCollection(CollectionField type);
	void VisitCustomField(CustomField type);
	void VisitTupleField(TupleField type);
}

public interface IFieldTypeVisitor<T>
{
	T VisitBasicField(BasicField type);
	T VisitCollection(CollectionField type);
	T VisitCustomField(CustomField type);
	T VisitTupleField(TupleField type);
}

public class FieldTypeVisitor : IFieldTypeVisitor
{
	public void Visit(IFieldType type) => type.Accept(this);

	public void VisitBasicField(BasicField type)
	{ }

	public void VisitCollection(CollectionField type) => Visit(type.BaseType);

	public void VisitCustomField(CustomField type) => Visit(type.BaseType);

	public void VisitTupleField(TupleField type)
	{
		foreach (var f in type.Fields) {
			Visit(f.Value);
		}
	}
}
