using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
public class StreamEvent : EventBase, IStreamEvent
{
	public StreamEvent(string streamName, string? message) : base(message)
	{
		StreamName = streamName;
	}

	public string StreamName { get; }
}
