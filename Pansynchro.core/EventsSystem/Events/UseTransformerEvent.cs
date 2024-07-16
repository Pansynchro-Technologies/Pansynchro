namespace Pansynchro.Core.EventsSystem.Events;
public class UseTransformerEvent : EventBase
{
	public UseTransformerEvent(string transformerName) : base(null)
	{
		TransformerName = transformerName;
	}

	public string TransformerName { get; }

	public override string ToString()
	{
		return $"{Name} {TransformerName}";
	}
}
