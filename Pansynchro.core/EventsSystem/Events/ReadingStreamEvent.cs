using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
public class ReadingStreamEvent : StreamEvent
{
	public ReadingStreamEvent(string streamName, string? message = null) : base(streamName, message)
	{
	}
}
