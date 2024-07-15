using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
public class TransformerMatchStreamEvent : StreamEvent
{
	public TransformerMatchStreamEvent(string transformerName, string streamName) : base(streamName, null)
	{
		TransformerName = transformerName;
	}

	public string TransformerName { get; }

	public override string ToString()
	{
		return $"{Name} {TransformerName} {StreamName}";
	}
}
