using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.PanSQL.Compiler.Test;
internal class CastTests
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

	private const string CAST_STRING_TO_GUID = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

stream guids as myDataDict.Guids
stream result as outDataDict.Guids

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

-- Uses a SQL query to convert text GUIDs into real GUIDs.
select g.Id, Cast(g.UniqueId as Guid) as UniqueId
from guids g
into result

sync myInput to myOutput";

	private const string CAST_STRING_TO_GUID_OUTPUT = @"using System;
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
			result[1] = Guid.Parse(r.GetString(1));
			yield return result;
		}
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Guids"", Transformer__1);
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""i1QBQJwHtiN6MORLzxHqdEt1AREZKjtbvlYhJMoVNubLpwKMjW7zuHx+Ba9HOu6OoUEQRJgEHZzLxzbVi3XTrIHXH1ghMPC3bflJMHrP0uayHnEEa2dEYT3tt+ALqAW/1FMQIBQAdIRQJpDq2ErYYzB3CcPg/eWH9IlkTyqlzY0LDIMrjYX1SDC5lqf+IDsSaXp7rcN28L6XTMMcPdThtBLGGgDbxn1TwzlvMgzmDUWXHnx0rQzc/a0zaP778EqzPL9suyQWRGGeSw28NDo+WG9h5wD3W8507lRpcdBVfZirHGdsme/feUz2jyG7C+KrzpIYFfCJljEsHjEA""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""CwICAIyULm8UHcyrjqUzgc2Ivunp/AGzcCqXsYrJ7927IB+45+NAhtMC4MytPWpUvd2cbi4DOQSd+SM2PKFHfVdPmA5QLVxUz5XEUIwCYZXwSNCBhLlgpoLzkyZsuv+p9A33aQt3SqT58rUWniVPMbfhrEVJdFzhHktapN255OrcFCUISStXoiiqQyzrYE5alxDxHBfJrGbwoCq7DXb3YdYN9gehW6uMOAS2b6xg5hV0ExYL/d/Yp4bju1M2PJeUJjaZagV6NT7H55YK8wTfnL5no/2mkzn52tXgVwl8dbWWq1o59TMtL0UIEqv1S9X+gnGey+jeKgRVNm8pnt0LWChd5/abvVyXc+pm+oUTqYvriKXlxtLCYi3WFlx8BC3/I0h8TWI=""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__2 = myInput.ReadFrom(myDataDict);
		reader__2 = new Sync(outDataDict).Transform(reader__2);
		await myOutput.Sync(reader__2, outDataDict);
	}
}
";

	[Test]
	public void ParseGuidCast()
	{
		var result = new Compiler().Compile("test", CAST_STRING_TO_GUID);
		Assert.That(result.Code, Is.EqualTo(FixDicts(CAST_STRING_TO_GUID_OUTPUT)));
	}
}
