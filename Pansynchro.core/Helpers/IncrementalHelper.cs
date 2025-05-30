using System.Collections.Generic;
using System.Linq;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.Incremental;

namespace Pansynchro.Core.Helpers
{
	public static class IncrementalHelper
	{
		public const string BOOKMARK_FIELD_NAME = "__incremental$bookmark__";
		public const string TYPE_FIELD_NAME = "__incremental$type__";
		public const string AFFECTED_FIELD_NAME = "__incremental$fields__";

		public static IAsyncEnumerable<DataStream> ReadSavedIncrementalData(
			IReader reader, DataDictionary dict, IEnumerable<StreamDescription> streams, int bookmarkLength)
		{
			var itr = new FileToIncrementalTransformer(bookmarkLength);
			var source = reader.ReadFrom(IncrementalDictFor(dict, streams.ToHashSet()));
			return itr.Transform(source);
		}

		internal static DataDictionary IncrementalDictFor(DataDictionary dict, HashSet<StreamDescription> streams)
		{
			var newStreams = dict.Streams
				.Select(s => IncrementalStream(s, streams))
				.ToArray();
			return dict with { Streams = newStreams };
		}

		private static readonly FieldDefinition[] INCREMENTAL_HEADER = {
			new(BOOKMARK_FIELD_NAME, new BasicField(TypeTag.Varchar, true, null, false)),
			new(TYPE_FIELD_NAME, new BasicField(TypeTag.Int, false, null, false)),
			new(AFFECTED_FIELD_NAME, new BasicField(TypeTag.Varchar, false, null, false)),
		};

		private static StreamDefinition IncrementalStream(StreamDefinition s, HashSet<StreamDescription> streams)
		{
			if (!streams.Contains(s.Name)) {
				return s;
			}

			return s with {
				Fields = INCREMENTAL_HEADER.Concat(s.Fields).ToArray(),
				SeqIdIndex = s.SeqIdIndex.HasValue ? s.SeqIdIndex + 3 : null,
				AuditFieldIndex = s.AuditFieldIndex.HasValue ? s.AuditFieldIndex + 3 : null,
			};
		}
	}
}
