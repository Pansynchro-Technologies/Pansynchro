using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;

public class TruncatingStreamEvent : StreamEvent
{
	public TruncatingStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
