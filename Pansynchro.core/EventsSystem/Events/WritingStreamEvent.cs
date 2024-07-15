using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
public class WritingStreamEvent : StreamEvent
{
	public WritingStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
