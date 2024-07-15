using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem.Events;
internal class WarningEvent : EventBase
{
	public WarningEvent(string message) : base(message)
	{
		LogLevel = Microsoft.Extensions.Logging.LogLevel.Warning;
	}
}