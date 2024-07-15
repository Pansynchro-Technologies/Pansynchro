using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;

public class EndSyncEvent : EventBase
{
	public EndSyncEvent(string? message = null) : base(message)
	{
	}
}

