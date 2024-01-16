using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Test
{
	public class FeatureCompleteness
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
open mySource as Files for source with '{ "Files": [ { "Name": "users", "File": ["\\myPath\\users*.avro"] } ] }'

-- opens a local file data sink
open mySink as Files for sink with '{ "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }'

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
	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
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
		_streamDict.Add(""Users"", Transformer__1);
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var mySource = ConnectorRegistry.GetSource(""Files"", ""{ \""Files\"": [ { \""Name\"": \""users\"", \""File\"": [\""\\\\myPath\\\\users*.avro\""] } ] }"");
		var mySink = ConnectorRegistry.GetSink(""Files"", ""{ \""Files\"": [ { \""StreamName\"": \""*\"", \""Filename\"": \""C:\\\\PansynchroData\\\\*.csv\"" } ], \""MissingFilenameSpec\"": \""C:\\\\PansynchroData\\\\Missing\\\\*.csv\"", \""DuplicateFilenameAction\"": 0 }"");
		var myInput = ConnectorRegistry.GetReader(""Avro"", ""connection string here"");
		((ISourcedConnector)myInput).SetDataSource(mySource);
		var myOutput = ConnectorRegistry.GetWriter(""CSV"", CredentialsFromEnv(""CsvConfigString""));
		((ISinkConnector)myOutput).SetDataSink(mySink);
		var reader__2 = myInput.ReadFrom(myDataDict);
		reader__2 = new Sync(outDataDict).Transform(reader__2);
		await myOutput.Sync(reader__2, outDataDict);
	}
}
";

		private const string SOURCE_CSPROJ = @"<Project Sdk=""Microsoft.NET.Sdk"">

	<PropertyGroup>
		<OutputType>Exe</OutputType>
		<TargetFramework>net8.0</TargetFramework>
		<Nullable>enable</Nullable>
		<CopyLocalLockFileAssemblies>true</CopyLocalLockFileAssemblies>
		<NoWarn>$(NoWarn);CS8621</NoWarn>
	</PropertyGroup>

	<ItemGroup>
		<PackageReference Include=""NMemory"" Version=""*"" />
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
open mySource as Files for source with '{ "Files": [ { "Name": "users", "File": ["\\myPath\\users*.avro"] } ] }'

-- opens a local file data sink
open mySink as Files for sink with '{ "Files": [ { "StreamName": "*", "Filename": "C:\\PansynchroData\\*.csv" } ], "MissingFilenameSpec": "C:\\PansynchroData\\Missing\\*.csv", "DuplicateFilenameAction": 0 }'

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
	}
}
