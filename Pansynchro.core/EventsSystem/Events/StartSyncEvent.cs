using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;

public class StartSyncEvent : EventBase
{
	public StartSyncEvent(string? message = null) : base(message)
	{
	}
}
