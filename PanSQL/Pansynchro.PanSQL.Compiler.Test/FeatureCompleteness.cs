﻿using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Test
{
	public class FeatureCompleteness
	{
		private string _inDict = null!;
		private string _outDict = null!;
		private string _nsDict = null!;

		[OneTimeSetUp]
		public void SetUp()
		{
			var inDict = DataDictionary.LoadFromFile("myDataDict.pansync");
			var outDict = DataDictionary.LoadFromFile("outDataDict.pansync");
			var nsDict = DataDictionary.LoadFromFile("nsDataDict.pansync");
			_inDict = inDict.ToString().ToCompressedString();
			_outDict = outDict.ToString().ToCompressedString();
			_nsDict = nsDict.ToString().ToCompressedString();
		}

		private string FixDicts(string expected) => expected.Replace("$INDICT$", _inDict).Replace("$OUTDICT$", _outDict).Replace("$NSDICT$", _nsDict);

		private const string ANALYZE = @"
--opens an analyzer of type MsSql with the provided connection string
open myInput as MsSql for analyze with 'connection string here'
analyze myInput as myDataDict with optimize --analyzes the database and produces the data dictionary

save myDataDict to '.\myDataDict.pansync' --saves the dict to the specified path
";

		private const string ANALYZE_OUTPUT = @"using System;
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
		var myInput = ConnectorRegistry.GetAnalyzer(""MSSQL"", ""connection string here"");
		var myDataDict = await myInput.AnalyzeAsync(""myDataDict"");
		myDataDict = await myInput.Optimize(myDataDict, _ => { });
		myDataDict.SaveToFile(FilenameHelper.Normalize("".\\myDataDict.pansync""));
	}
}
";

		[Test]
		public void ParseAnalyzeRequest()
		{
			var result = new Compiler().Compile("test", ANALYZE);
			Assert.That(result.Code, Is.EqualTo(FixDicts(ANALYZE_OUTPUT)));
		}

		private const string ANALYZE2 = @"
--opens an analyzer of type MsSql with the provided connection string
open myInput as MsSql for analyze with 'connection string here'
analyze myInput as myDataDict with optimize, include(Users, UserTypes) --analyzes the database and produces the data dictionary

save myDataDict to '.\myDataDict.pansync' --saves the dict to the specified path
";

		private const string ANALYZE2_OUTPUT = @"using System;
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
		var myInput = ConnectorRegistry.GetAnalyzer(""MSSQL"", ""connection string here"");
		var myDataDict = await ((IQueryableSchemaAnalyzer)myInput).AnalyzeAsync(""myDataDict"", [""Users"", ""UserTypes""]);
		myDataDict = await myInput.Optimize(myDataDict, _ => { });
		myDataDict.SaveToFile(FilenameHelper.Normalize("".\\myDataDict.pansync""));
	}
}
";

		[Test]
		public void ParseAnalyzeRequest2()
		{
			var result = new Compiler().Compile("test", ANALYZE2);
			Assert.That(result.Code, Is.EqualTo(FixDicts(ANALYZE2_OUTPUT)));
		}

		private const string ANALYZE3 = @"
--opens an analyzer of type MsSql with the provided connection string
open myInput as MsSql for analyze with 'connection string here'
analyze myInput as myDataDict with optimize, exclude(Users, UserTypes) --analyzes the database and produces the data dictionary

save myDataDict to '.\myDataDict.pansync' --saves the dict to the specified path
";

		private const string ANALYZE3_OUTPUT = @"using System;
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
		var myInput = ConnectorRegistry.GetAnalyzer(""MSSQL"", ""connection string here"");
		var myDataDict = await ((IQueryableSchemaAnalyzer)myInput).AnalyzeExcludingAsync(""myDataDict"", [""Users"", ""UserTypes""]);
		myDataDict = await myInput.Optimize(myDataDict, _ => { });
		myDataDict.SaveToFile(FilenameHelper.Normalize("".\\myDataDict.pansync""));
	}
}
";

		[Test]
		public void ParseAnalyzeRequest3()
		{
			var result = new Compiler().Compile("test", ANALYZE3);
			Assert.That(result.Code, Is.EqualTo(FixDicts(ANALYZE3_OUTPUT)));
		}

		private const string SOURCE = """
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream users as myDataDict.Users
stream users2 as outDataDict.Users

-- opens a local file data source
open mySource as Files for source with { "Files": [ { "Name": "users", "File": ["\\myPath\\users*.avro"] } ] }

-- opens a local file data sink
open mySink as Files for sink with { "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }

--opens a reader of type Avro with the provided connection string, associated with myDataDict, and using mySource for the data source
open myInput as Avro for read with myDataDict, 'connection string here', mySource

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Csv for write with outDataDict, CredentialsFromEnv('CsvConfigString'), mySink

select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
""";

		private const string SOURCE_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[6];
		result[3] = ""NONE"";
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
		_streamDict.Add(""Users"", Transformer__2);
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var mySource = ConnectorRegistry.GetSource(""Files"", ""{\""Files\"":[{\""Name\"":\""users\"",\""File\"":[\""\\\\myPath\\\\users*.avro\""]}]}"");
		var mySink = ConnectorRegistry.GetSink(""Files"", ""{\""Files\"":[{\""StreamName\"":\""*\"",\""Filename\"":\""C:\\\\PansynchroData\\\\*.csv\""}],\""MissingFilenameSpec\"":\""C:\\\\PansynchroData\\\\Missing\\\\*.csv\"",\""DuplicateFilenameAction\"":0}"");
		var myInput = ConnectorRegistry.GetReader(""Avro"", ""connection string here"");
		((ISourcedConnector)myInput).SetDataSource(mySource);
		var myOutput = ConnectorRegistry.GetWriter(""CSV"", CredentialsFromEnv(""CsvConfigString""));
		((ISinkConnector)myOutput).SetDataSink(mySink);
		var reader__1 = myInput.ReadFrom(myDataDict);
		reader__1 = new Sync(outDataDict).Transform(reader__1);
		await myOutput.Sync(reader__1, outDataDict);
	}
}
";

		private const string SOURCE_CSPROJ = @"<Project Sdk=""Microsoft.NET.Sdk"">

	<PropertyGroup>
		<OutputType>Exe</OutputType>
		<TargetFramework>net9.0</TargetFramework>
		<Nullable>enable</Nullable>
		<CopyLocalLockFileAssemblies>true</CopyLocalLockFileAssemblies>
		<NoWarn>$(NoWarn);CS8621</NoWarn>
	</PropertyGroup>

	<ItemGroup>
		<PackageReference Include=""Pansynchro.Core"" Version=""*"" />
		<PackageReference Include=""Pansynchro.PanSQL.Core"" Version=""*"" />
		<PackageReference Include=""Pansynchro.Sources.Files"" Version=""*"" />
		<PackageReference Include=""Pansynchro.Connectors.Avro"" Version=""*"" />
		<PackageReference Include=""Pansynchro.Connectors.TextFile"" Version=""*"" />
	</ItemGroup>

	<ItemGroup>
		<None Update=""connectors.pansync"">
			<CopyToOutputDirectory>Always</CopyToOutputDirectory>
		</None>
	</ItemGroup>
	
</Project>
";

		[Test]
		public void ParseSourceSink()
		{
			var result = new Compiler().Compile("test", SOURCE);
			Assert.Multiple(() => {
				Assert.That(result.Code, Is.EqualTo(FixDicts(SOURCE_OUTPUT)));
				Assert.That(result.ProjectFile, Is.EqualTo(FixDicts(SOURCE_CSPROJ)));
			});
		}

		private const string BAD_SOURCE = """
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\outDataDict.pansync'

stream users as myDataDict.Users
stream users2 as outDataDict.Users

-- opens a local file data source
open mySource as Files for source with { "Files": [ { "Name": "users", "File": ["\\myPath\\users*.avro"] } ] }

-- opens a local file data sink
open mySink as Files for sink with { "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }

--opens a reader of type Avro with the provided connection string, associated with myDataDict, and using mySource for the data source
open myInput as MsSql for read with myDataDict, 'connection string here', mySource

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Csv for write with outDataDict, CredentialsFromEnv('CsvConfigString'), mySink

select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
""";

		[Test]
		public void RejectIncorrectSource()
		{
			var err = Assert.Throws<CompilerError>(() => new Compiler().Compile("test", BAD_SOURCE));
			Assert.That(err.Message, Is.EqualTo("The 'MSSQL' connector does not use a data source."));
		}

		private const string NS_MAP = """
load myDataDict from '.\myDataDict.pansync'
load outDataDict from '.\nsDataDict.pansync'

open myInput as Firebird for read with myDataDict, 'connection string here'

open myOutput as MSSQL for write with outDataDict, CredentialsFromEnv('CsvConfigString')

map namespace null to dbo

sync myInput to myOutput
""";

		private const string NS_MAP_OUTPUT = """
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	public Sync(DataDictionary destDict) : base(destDict) {
		MapNamespaces(null, "dbo");
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$INDICT$"));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress("$NSDICT$"));
		var myInput = ConnectorRegistry.GetReader("Firebird", "connection string here");
		var myOutput = ConnectorRegistry.GetWriter("MSSQL", CredentialsFromEnv("CsvConfigString"));
		var reader__1 = myInput.ReadFrom(myDataDict);
		reader__1 = new Sync(outDataDict).Transform(reader__1);
		await myOutput.Sync(reader__1, outDataDict);
	}
}

""";
		[Test]
		public void NsMap()
		{
			var result = new Compiler().Compile("test", NS_MAP);
			Assert.That(result.Code, Is.EqualTo(FixDicts(NS_MAP_OUTPUT)));
		}

		private const string PURE_FILTER = """
load dict from '.\myDataDict.pansync'
open inFile as Files for source with '{ "Files": [{ "Name": "input", "File": ["InputFile.csv"] }]"}'
open outFile as Files for sink with '{ "Files": [{ "Name": "output", "File": ["OutputFile.csv"] }]"}'
open input as CSV for read with dict, 'Delimiter=	', inFile
open output as CSV for write with dict, 'Delimiter=	', outFile

stream inUsers as dict.Users
stream outUsers as dict.Users

select Id, Name, Address, TypeID, AccountId, EmailHash
from inUsers u
where u.Address is not null
into outUsers

sync input to output
""";

		private const string PURE_FILTER_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

class Sync : StreamTransformerBase {
	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		var result = new object[6];
		while (r.Read()) {
			if (!((!(((r.IsDBNull(2) ? System.DBNull.Value : r.GetString(2)) == System.DBNull.Value))))) continue;
			result[0] = r.GetInt32(0);
			result[1] = r.GetString(1);
			result[2] = (r.IsDBNull(2) ? System.DBNull.Value : r.GetString(2));
			result[3] = r.GetInt32(3);
			result[4] = (r.IsDBNull(4) ? System.DBNull.Value : r.GetInt32(4));
			result[5] = (r.IsDBNull(5) ? System.DBNull.Value : r.GetString(5));
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Users"", Transformer__2);
	}
}

static class Program {
	public static async Task Main() {
		var dict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""i1QBQJwHtiN6MORLzxHqdEt1AREZKjtbvlYhJMoVNubLpwKMjW7zuHx+Ba9HOu6OoUEQRJgEHZzLxzbVi3XTrIHXH1ghMPC3bflJMHrP0uayHnEEa2dEYT3tt+ALqAW/1FMQIBQAdIRQJpDq2ErYYzB3CcPg/eWH9IlkTyqlzY0LDIMrjYX1SDC5lqf+IDsSaXp7rcN28L6XTMMcPdThtBLGGgDbxn1TwzlvMgzmDUWXHnx0rQzc/a0zaP778EqzPL9suyQWRGGeSw28NDo+WG9h5wD3W8507lRpcdBVfZirHGdsme/feUz2jyG7C+KrzpIYFfCJljEsHjEA""));
		var inFile = ConnectorRegistry.GetSource(""Files"", ""{ \""Files\"": [{ \""Name\"": \""input\"", \""File\"": [\""InputFile.csv\""] }]\""}"");
		var outFile = ConnectorRegistry.GetSink(""Files"", ""{ \""Files\"": [{ \""Name\"": \""output\"", \""File\"": [\""OutputFile.csv\""] }]\""}"");
		var input = ConnectorRegistry.GetReader(""CSV"", ""Delimiter=\t"");
		((ISourcedConnector)input).SetDataSource(inFile);
		var output = ConnectorRegistry.GetWriter(""CSV"", ""Delimiter=\t"");
		((ISinkConnector)output).SetDataSink(outFile);
		var reader__1 = input.ReadFrom(dict);
		reader__1 = new Sync(dict).Transform(reader__1);
		await output.Sync(reader__1, dict);
	}
}
";

		[Test]
		public void PureFilter()
		{
			var result = new Compiler().Compile("test", PURE_FILTER);
			Assert.That(result.Code, Is.EqualTo(FixDicts(PURE_FILTER_OUTPUT)));
		}
	}
}
