using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
public class StartSyncStreamEvent : StreamEvent
{
	public StartSyncStreamEvent(string streamName) : base(streamName, null)
	{
	}
}
