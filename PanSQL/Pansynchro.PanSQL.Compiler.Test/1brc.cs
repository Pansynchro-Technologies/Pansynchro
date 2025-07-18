﻿using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Test
{
	internal class _1brc
	{
		private string _inDict = null!;
		private string _outDict = null!;

		[OneTimeSetUp]
		public void SetUp()
		{
			var inDict = DataDictionary.LoadFromFile("1brc.pansync");
			var outDict = DataDictionary.LoadFromFile("1brc_results.pansync");
			_inDict = inDict.ToString().ToCompressedString();
			_outDict = outDict.ToString().ToCompressedString();
		}

		private string FixDicts(string expected) => expected.Replace("$INDICT$", _inDict).Replace("$OUTDICT$", _outDict);

		private const string BRC_SCRIPT = @"
load dataDict from '.\1brc.pansync'
load resultDict from '.\1brc_results.pansync'

open mySource as Files for source with { ""Files"": [ { ""Name"": ""Data"", ""File"": [""measurements.txt""] } ] }
open myInput as Csv for read with dataDict, 'Delimiter='';''', mySource
open myOutput as Console for write with resultDict, ''

stream data as dataDict.Data
stream result as resultDict.Result

with aggs as (
	select Name, min(Temperature) as minTemp, avg(Temperature) as meanTemp, max(Temperature) as maxTemp
	from data
	group by Name
)
select '{' + string_agg(format('{0}={1:F1}/{2:F1}/{3:F1}', Name, MinTemp, MeanTemp, MaxTemp), ', ') + '}'
from aggs
order by Name
into result

sync myInput to myOutput
";

		private const string BRC_SCRIPT_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	internal class DB {
		public class Aggs_ {
			public string Name { get; }
			public float MinTemp { get; }
			public float MeanTemp { get; }
			public float MaxTemp { get; }

			public Aggs_(string name_, float mintemp_, float meantemp_, float maxtemp_) {
				this.Name = name_;
				this.MinTemp = mintemp_;
				this.MeanTemp = meantemp_;
				this.MaxTemp = maxtemp_;
			}
		}

		public List<Aggs_> Aggs { get; }

		public DB() {
			Aggs = [];
		}
	}

	public static readonly DB __db = new();

	private void Transformer__2(IDataReader r) {
		var aggregator__3 = Aggregates.Min<string, float>();
		var aggregator__4 = Aggregates.Avg<string, float>();
		var aggregator__5 = Aggregates.Max<string, float>();
		while (r.Read()) {
			aggregator__3.Add(r.GetString(0), r.GetFloat(1));
			aggregator__4.Add(r.GetString(0), r.GetFloat(1));
			aggregator__5.Add(r.GetString(0), r.GetFloat(1));
		}
		foreach (var pair in Aggregates.Combine(aggregator__3, aggregator__4, aggregator__5)) {
			__db.Aggs.Add(new DB.Aggs_(pair.Key, pair.Value.Item1, pair.Value.Item2, pair.Value.Item3));
		}
	}

	private IEnumerable<object?[]> Transformer__6(IDataReader r) {
		Transformer__2(r);
		var result = new object[1];
		var __preAgg = from __Aggs in __db.Aggs orderby __Aggs.Name select __Aggs;
		var aggregator__7 = Aggregates.String_agg<bool, string>("", "");
		foreach (var __item in __preAgg) {
			aggregator__7.Add(true, String.Format(""{0}={1:F1}/{2:F1}/{3:F1}"", __item.Name, __item.MinTemp, __item.MeanTemp, __item.MaxTemp));
		}
		foreach (var pair in aggregator__7) {
			result[0] = ""{"" + pair.Value + ""}"";
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Data"", Transformer__6);
		_nameMap.Add(StreamDescription.Parse(""Data""), StreamDescription.Parse(""Result""));
	}
}

static class Program {
	public static async Task Main() {
		var dataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var resultDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var mySource = ConnectorRegistry.GetSource(""Files"", ""{\""Files\"":[{\""Name\"":\""Data\"",\""File\"":[\""measurements.txt\""]}]}"");
		var myInput = ConnectorRegistry.GetReader(""CSV"", ""Delimiter=';'"");
		((ISourcedConnector)myInput).SetDataSource(mySource);
		var myOutput = ConnectorRegistry.GetWriter(""Console"", """");
		var reader__1 = myInput.ReadFrom(dataDict);
		reader__1 = new Sync(resultDict).Transform(reader__1);
		await myOutput.Sync(reader__1, resultDict);
	}
}
";

		[Test]
		public void ParseBrc()
		{
			var result = new Compiler().Compile("test", BRC_SCRIPT);
			Assert.That(result.Code, Is.EqualTo(FixDicts(BRC_SCRIPT_OUTPUT)));
		}
	}
}
