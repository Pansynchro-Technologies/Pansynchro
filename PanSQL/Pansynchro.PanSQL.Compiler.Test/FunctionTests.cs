using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;


namespace Pansynchro.PanSQL.Compiler.Test
{
	public class FunctionTests
	{
		private string _inDict = null!;
		private string _outDict = null!;

		[OneTimeSetUp]
		public void SetUp()
		{
			var inDict = DataDictionary.LoadFromFile("myDataDict.pansync");
			var outDict = DataDictionary.LoadFromFile("outDataDict.pansync");
			_inDict = inDict.ToString().ToCompressedString();
			_outDict = outDict.ToString().ToCompressedString();
		}

		private string FixDicts(string expected) => expected.Replace("$INDICT$", _inDict).Replace("$OUTDICT$", _outDict);

		private const string FORMATTING = @"
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream products as myDataDict.Products
stream products2 as outDataDict.Products

open myInput as MsSql for read with myDataDict, format('connection string {0}', 'here')
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

sync myInput to myOutput
";

		private const string FORMATTING_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", (String.Format(""connection string {0}"", ""here"")));
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__1 = myInput.ReadFrom(myDataDict);
		await myOutput.Sync(reader__1, outDataDict);
	}
}
";

		[Test]
		public void Format()
		{
			var result = new Compiler().Compile("test", FORMATTING);
			Assert.That(result.Code, Is.EqualTo(FixDicts(FORMATTING_OUTPUT)));
		}

		private const string CURRENT_TIMESTAMP = @"
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream Orders as myDataDict.Orders
stream OrderData as outDataDict.OrderData

open myInput as MsSql for read with myDataDict, 'connection string here'

open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select o.Id, CURRENT_TIMESTAMP
from Orders o
into OrderData

sync myInput to myOutput";

		private const string CURRENT_TIMESTAMP_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		var result = new object[2];
		while (r.Read()) {
			result[0] = r.GetInt32(0);
			result[1] = DateTime.Now;
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Orders"", Transformer__1);
		_nameMap.Add(StreamDescription.Parse(""Orders""), StreamDescription.Parse(""OrderData""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__2 = myInput.ReadFrom(myDataDict);
		reader__2 = new Sync(outDataDict).Transform(reader__2);
		await myOutput.Sync(reader__2, outDataDict);
	}
}
";

		[Test]
		public void CurrentTimestamp()
		{
			var result = new Compiler().Compile("test", CURRENT_TIMESTAMP);
			Assert.That(result.Code, Is.EqualTo(FixDicts(CURRENT_TIMESTAMP_OUTPUT)));
		}

		private const string GETDATE = @"
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream Orders as myDataDict.Orders
stream OrderData as outDataDict.OrderData

open myInput as MsSql for read with myDataDict, 'connection string here'

open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select o.Id, GETDATE()
from Orders o
into OrderData

sync myInput to myOutput";

		[Test]
		public void GetDate()
		{
			var result = new Compiler().Compile("test", GETDATE);
			Assert.That(result.Code, Is.EqualTo(FixDicts(CURRENT_TIMESTAMP_OUTPUT)));
		}

		private const string GETUTCDATE = @"
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream Orders as myDataDict.Orders
stream OrderData as outDataDict.OrderData

open myInput as MsSql for read with myDataDict, 'connection string here'

open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select o.Id, GETUTCDATE()
from Orders o
into OrderData

sync myInput to myOutput";
		
		private const string GETUTCDATE_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		var result = new object[2];
		while (r.Read()) {
			result[0] = r.GetInt32(0);
			result[1] = DateTime.UtcNow;
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Orders"", Transformer__1);
		_nameMap.Add(StreamDescription.Parse(""Orders""), StreamDescription.Parse(""OrderData""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__2 = myInput.ReadFrom(myDataDict);
		reader__2 = new Sync(outDataDict).Transform(reader__2);
		await myOutput.Sync(reader__2, outDataDict);
	}
}
";

		[Test]
		public void GetUtcDate()
		{
			var result = new Compiler().Compile("test", GETUTCDATE);
			Assert.That(result.Code, Is.EqualTo(FixDicts(GETUTCDATE_OUTPUT)));
		}

	}
}
