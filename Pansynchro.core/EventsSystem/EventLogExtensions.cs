using Pansynchro.Core.EventsSystem.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.EventsSystem;
public static class EventLogExtensions
{
	public static void AddStartSyncEvent(this EventLog eventlog)
	{
		eventlog.AddEvent(new StartSyncEvent());
	}

	public static void AddStartSyncStreamEvent(this EventLog eventlog, StreamDescription stream)
	{
		eventlog.AddEvent(new StartSyncStreamEvent(stream.ToString()));
	}

	public static void AddReadingStreamEvent(this EventLog eventlog, StreamDescription stream, string? message)
	{
		eventlog.AddEvent(new ReadingStreamEvent(stream.ToString(), message));
	}

	public static void AddWritingStreamEvent(this EventLog eventlog, StreamDescription stream)
	{
		eventlog.AddEvent(new WritingStreamEvent(stream.ToString()));
	}

	public static void AddEndSyncStreamEvent(this EventLog eventlog, StreamDescription stream)
	{
		eventlog.AddEvent(new EndSyncStreamEvent(stream.ToString()));
	}

	public static void AddMergingStreamEvent(this EventLog eventlog, StreamDescription stream)
	{
		eventlog.AddEvent(new MergingStreamEvent(stream.ToString()));
	}

	public static void AddTruncatingStreamEvent(this EventLog eventlog, StreamDescription stream)
	{
		eventlog.AddEvent(new TruncatingStreamEvent(stream.ToString()));
	}

	public static void AddEndSyncEvent(this EventLog eventlog)
	{
		eventlog.AddEvent(new EndSyncEvent());
	}

	public static void AddUseTransformerEvent(this EventLog eventlog, ITransformer tr)
	{
		eventlog.AddEvent(new UseTransformerEvent(tr.GetType().FullName!));
	}

	public static void AddTransformerMatchStreamEvent(this EventLog eventlog, ITransformer tr, StreamDescription stream)
	{
		eventlog.AddEvent(new TransformerMatchStreamEvent(tr.GetType().FullName!, stream.ToString()));
	}

	public static void AddInformationEvent(this EventLog eventLog, string message)
	{
		eventLog.AddEvent(new InformationEvent(message));
	}

	public static void AddWarningEvent(this EventLog eventlog, string message)
	{
		eventlog.AddEvent(new WarningEvent(message));
	}

	public static void AddErrorEvent(this EventLog eventLog, Exception? exception, StreamDescription? stream = null, string? message = null)
	{
		eventLog.AddEvent(new ErrorEvent(exception, stream, message));
	}
}

