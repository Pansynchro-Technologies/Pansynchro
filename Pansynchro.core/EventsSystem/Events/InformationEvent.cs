using Pansynchro.Core.EventsSystem.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;

public class InformationEvent : EventBase
{
	public InformationEvent(string message) : base(message)
	{
		LogLevel = Microsoft.Extensions.Logging.LogLevel.Information;
	}
}
