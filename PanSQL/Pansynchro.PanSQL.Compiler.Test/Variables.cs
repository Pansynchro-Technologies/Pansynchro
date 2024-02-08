using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Test
{
	internal class Variables
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

		private const string DECLARE = @"
declare @conn text
open myInput as MsSql for analyze with @conn
analyze myInput as myDataDict with optimize --analyzes the database and produces the data dictionary
save myDataDict to '.\myDataDict.pansync' --saves the dict to the specified path
";

		private const string DECLARE_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

static class Program {
	public static async Task Main(string[] args) {
		string conn__1;
		var __varResult = new VariableReader(args, true).ReadVar(""conn"", out conn__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myInput = ConnectorRegistry.GetAnalyzer(""MSSQL"", conn__1);
		var myDataDict = await myInput.AnalyzeAsync(""myDataDict"");
		myDataDict = await myInput.Optimize(myDataDict, _ => { });
		myDataDict.SaveToFile(FilenameHelper.Normalize("".\\myDataDict.pansync""));
	}
}
";

		[Test]
		public void DeclareVariables()
		{
			var result = new Compiler().Compile("test", DECLARE);
			Assert.That(result.Code, Is.EqualTo(DECLARE_OUTPUT));
		}

		private const string DECLARE_WRONG_TYPE = @"
declare @conn int
open myInput as MsSql for analyze with @conn
analyze myInput as myDataDict with optimize --analyzes the database and produces the data dictionary
save myDataDict to '.\myDataDict.pansync' --saves the dict to the specified path
";

		[Test]
		public void DeclareWrongType()
		{
			var err = Assert.Throws<CompilerError>(() => new Compiler().Compile("test", DECLARE_WRONG_TYPE));
			Assert.That(err?.Message, Is.EqualTo("'conn__1' is not a string type"));
		}

		private const string SQL_VAR = @"
declare @sku Ntext

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream products as myDataDict.Products
stream products2 as outDataDict.Products

open myInput as MsSql for read with myDataDict, 'connection string here'
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.SKU = @sku
into products2

sync myInput to myOutput
";

		private const string SQL_VAR_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private readonly string _sku;

	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[4];
		while (r.Read()) {
			if (!((r.GetString(1) == _sku))) continue;
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = r.GetInt32(2);
			result[3] = r.GetDecimal(3);
			yield return result;
		}
	}

	public Sync(DataDictionary destDict, string sku) : base(destDict) {
		_sku = sku;
		_streamDict.Add(""Products"", Transformer__2);
	}
}

static class Program {
	public static async Task Main(string[] args) {
		string sku__1;
		var __varResult = new VariableReader(args, true).ReadVar(""sku"", out sku__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict, sku__1).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void UseVariablesInSql()
		{
			var result = new Compiler().Compile("test", SQL_VAR);
			Assert.That(result.Code, Is.EqualTo(FixDicts(SQL_VAR_OUTPUT)));
		}

		private const string SQL_INT_VAR = @"
declare @id int

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream products as myDataDict.Products
stream products2 as outDataDict.Products

open myInput as MsSql for read with myDataDict, 'connection string here'
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.SKU = 'ABC123' and p.id >= @id
into products2

sync myInput to myOutput
";

		private const string SQL_INT_VAR_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private readonly int _id;

	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[4];
		while (r.Read()) {
			if (!((r.GetString(1) == ""ABC123"" && r.GetInt32(0) >= _id))) continue;
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = r.GetInt32(2);
			result[3] = r.GetDecimal(3);
			yield return result;
		}
	}

	public Sync(DataDictionary destDict, int id) : base(destDict) {
		_id = id;
		_streamDict.Add(""Products"", Transformer__2);
	}
}

static class Program {
	public static async Task Main(string[] args) {
		int id__1;
		var __varResult = new VariableReader(args, true).ReadVar(""id"", out id__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict, id__1).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void UseIntVariableInSql()
		{
			var result = new Compiler().Compile("test", SQL_INT_VAR);
			Assert.That(result.Code, Is.EqualTo(FixDicts(SQL_INT_VAR_OUTPUT)));
		}

		private const string SQL_INT_VAR_DEFAULT = @"
declare @id int = 78

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream products as myDataDict.Products
stream products2 as outDataDict.Products

open myInput as MsSql for read with myDataDict, 'connection string here'
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.SKU = 'ABC123' and p.id >= @id
into products2

sync myInput to myOutput
";

		private const string SQL_INT_VAR_DEFAULT_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private readonly int _id;

	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[4];
		while (r.Read()) {
			if (!((r.GetString(1) == ""ABC123"" && r.GetInt32(0) >= _id))) continue;
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = r.GetInt32(2);
			result[3] = r.GetDecimal(3);
			yield return result;
		}
	}

	public Sync(DataDictionary destDict, int id) : base(destDict) {
		_id = id;
		_streamDict.Add(""Products"", Transformer__2);
	}
}

static class Program {
	public static async Task Main(string[] args) {
		int id__1 = 78;
		var __varResult = new VariableReader(args, false).TryReadVar(""id"", ref id__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict, id__1).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void UseIntVariableWithDefault()
		{
			var result = new Compiler().Compile("test", SQL_INT_VAR_DEFAULT);
			Assert.That(result.Code, Is.EqualTo(FixDicts(SQL_INT_VAR_DEFAULT_OUTPUT)));
		}

		private const string SQLVAR_WRONG_TYPE = @"
declare @sku int

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream products as myDataDict.Products
stream products2 as outDataDict.Products

open myInput as MsSql for read with myDataDict, 'connection string here'
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.SKU = @sku
into products2

sync myInput to myOutput
";

		[Test]
		public void SqlVarWrongType()
		{
			var err = Assert.Throws<CompilerError>(() => new Compiler().Compile("test", SQLVAR_WRONG_TYPE));
			Assert.That(err?.Message, Is.EqualTo("Incompatible types in expression 'products.SKU == _sku':  'Nvarchar(20)' and 'Int'"));
		}

		private const string JSON_WITH_VAR_OBJ = """
declare @streamname text

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream users as myDataDict.Users
stream users2 as outDataDict.Users

open mySource as Files for source with { "Files": [ { "Name": @streamname, "File": ["\\myPath\\users*.avro"] } ] }
open mySink as Files for sink with { "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }
open myInput as Avro for read with myDataDict, 'connection string here', mySource
open myOutput as Csv for write with outDataDict, CredentialsFromEnv('CsvConfigString'), mySink

select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
""";

		private const string JSON_WITH_VAR_OBJ_OUTPUT = """
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[6];
		result[3] = "NONE";
		result[4] = DBNull.Value;
		result[5] = DBNull.Value;
		while (r.Read()) {
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = (r.IsDBNull(2) ? System.DBNull.Value : r.GetString(2));
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add("Users", Transformer__2);
	}
}

static class Program {
	public static async Task Main(string[] args) {
		string streamname__1;
		var __varResult = new VariableReader(args, true).ReadVar("streamname", out streamname__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$INDICT$"));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$OUTDICT$"));
		var json__3 = JsonNode.Parse("{\"Files\":[{\"File\":[\"\\\\myPath\\\\users*.avro\"]}]}")!;
		json__3["Files"]![0]!["Name"] = streamname__1;
		var mySource = ConnectorRegistry.GetSource("Files", json__3.ToJsonString());
		var mySink = ConnectorRegistry.GetSink("Files", "{\"Files\":[{\"StreamName\":\"*\",\"Filename\":\"C:\\\\PansynchroData\\\\*.csv\"}],\"MissingFilenameSpec\":\"C:\\\\PansynchroData\\\\Missing\\\\*.csv\",\"DuplicateFilenameAction\":0}");
		var myInput = ConnectorRegistry.GetReader("Avro", "connection string here");
		((ISourcedConnector)myInput).SetDataSource(mySource);
		var myOutput = ConnectorRegistry.GetWriter("CSV", CredentialsFromEnv("CsvConfigString"));
		((ISinkConnector)myOutput).SetDataSink(mySink);
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}

""";

		[Test]
		public void ObjectVariablesInJsonLiteral()
		{
			var result = new Compiler().Compile("test", JSON_WITH_VAR_OBJ);
			Assert.That(result.Code, Is.EqualTo(FixDicts(JSON_WITH_VAR_OBJ_OUTPUT)));
		}

		private const string JSON_WITH_VAR_ARR = """
declare @filename text

load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream users as myDataDict.Users
stream users2 as outDataDict.Users

open mySource as Files for source with { "Files": [ { "Name": "files", "File": [@filename] } ] }
open mySink as Files for sink with { "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }
open myInput as Avro for read with myDataDict, 'connection string here', mySource
open myOutput as Csv for write with outDataDict, CredentialsFromEnv('CsvConfigString'), mySink

select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
""";

		private const string JSON_WITH_VAR_ARR_OUTPUT = """
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[6];
		result[3] = "NONE";
		result[4] = DBNull.Value;
		result[5] = DBNull.Value;
		while (r.Read()) {
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = (r.IsDBNull(2) ? System.DBNull.Value : r.GetString(2));
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add("Users", Transformer__2);
	}
}

static class Program {
	public static async Task Main(string[] args) {
		string filename__1;
		var __varResult = new VariableReader(args, true).ReadVar("filename", out filename__1).Result;
		if (__varResult != null) {
			System.Console.WriteLine(__varResult);
			return;
		}
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$INDICT$"));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$OUTDICT$"));
		var json__3 = JsonNode.Parse("{\"Files\":[{\"Name\":\"files\",\"File\":[]}]}")!;
		((JsonArray)json__3["Files"]![0]!["File"]!).Insert(0, filename__1);
		var mySource = ConnectorRegistry.GetSource("Files", json__3.ToJsonString());
		var mySink = ConnectorRegistry.GetSink("Files", "{\"Files\":[{\"StreamName\":\"*\",\"Filename\":\"C:\\\\PansynchroData\\\\*.csv\"}],\"MissingFilenameSpec\":\"C:\\\\PansynchroData\\\\Missing\\\\*.csv\",\"DuplicateFilenameAction\":0}");
		var myInput = ConnectorRegistry.GetReader("Avro", "connection string here");
		((ISourcedConnector)myInput).SetDataSource(mySource);
		var myOutput = ConnectorRegistry.GetWriter("CSV", CredentialsFromEnv("CsvConfigString"));
		((ISinkConnector)myOutput).SetDataSink(mySink);
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}

""";

		[Test]
		public void ArrayVariablesInJsonLiteral()
		{
			var result = new Compiler().Compile("test", JSON_WITH_VAR_ARR);
			Assert.That(result.Code, Is.EqualTo(FixDicts(JSON_WITH_VAR_ARR_OUTPUT)));
		}

		private const string JSON_WITH_MISSING_VAR = """
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream users as myDataDict.Users
stream users2 as outDataDict.Users

open mySource as Files for source with { "Files": [ { "Name": @streamname, "File": ["\\myPath\\users*.avro"] } ] }
open mySink as Files for sink with { "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }
open myInput as Avro for read with myDataDict, 'connection string here', mySource
open myOutput as Csv for write with outDataDict, CredentialsFromEnv('CsvConfigString'), mySink

select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
""";

		[Test]
		public void MissingVariablesInJsonLiteral()
		{
			var err = Assert.Throws<CompilerError>(() => new Compiler().Compile("test", JSON_WITH_MISSING_VAR));
			Assert.That(err?.Message, Is.EqualTo("No variable named 'streamname' has been defined"));
		}
	}
}
