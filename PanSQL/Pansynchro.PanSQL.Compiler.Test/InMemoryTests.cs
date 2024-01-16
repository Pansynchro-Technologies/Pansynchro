using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Test
{
	public class InMemoryTests
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

		private const string MISSING_FIELD_SPEC = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table types as myDataDict.UserTypes
table users as myDataDict.Users
stream users2 as outDataDict.Users

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data from multiple sources.
  All JOINed tables must be declared as table, not stream.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select u.name, u.address, t.name as type
from users u
join types t on u.typeId = t.Id
into users2

map myDataDict.Orders to outDataDict.OrderData --renames one stream to another
map myDataDict.Products to outDataDict.Products with {SKU = name, Vendor = VendorId} --renames fields within a stream

sync myInput to myOutput
/*At EOF:
  Any streams not mentioned in input or output are auto-mapped to one another by equal names.
  All inputs and outputs are type-checked.  Errors are emitted as needed.
  Warnings are emitted for unused streams
  input streams declared as Table are not auto-mapped.  If manually mapped, transfer will work as expected
  Run codegen */";

		[Test]
		public void ParseMissingFieldSpec()
		{
			var err = Assert.Throws<CompilerError>(() => new Compiler().Compile("test", MISSING_FIELD_SPEC));
			Assert.That(err.Message, Is.EqualTo("The following field(s) on users2 are not nullable, but are not assigned a value: Id"));
		}

		private const string CLEAN_SPEC = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table types as myDataDict.UserTypes
table users as myDataDict.Users
stream users2 as outDataDict.Users

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data from multiple sources.
  All JOINed tables must be declared as table, not stream.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select u.id, u.name, u.address, t.name as type
from users u
join types t on u.typeId = t.Id
into users2

map myDataDict.Orders to outDataDict.OrderData --renames one stream to another
map myDataDict.Products to outDataDict.Products with {SKU = name, Vendor = VendorId} --renames fields within a stream

sync myInput to myOutput
/*At EOF:
  Any streams not mentioned in input or output are auto-mapped to one another by equal names.
  All inputs and outputs are type-checked.  Errors are emitted as needed.
  Warnings are emitted for unused streams
  input streams declared as Table are not auto-mapped.  If manually mapped, transfer will work as expected
  Run codegen */";

		private const string CLEAN_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class UserTypes_ {
			public int Id { get; }
			public string Name { get; }

			public UserTypes_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.Name = r.GetString(1);
			}
		}

		public class Users_ {
			public int Id { get; }
			public string Name { get; }
			public string? Address { get; }
			public int TypeID { get; }
			public int? AccountId { get; }
			public string? EmailHash { get; }

			public Users_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.Name = r.GetString(1);
				this.Address = r.IsDBNull(2) ? null : r.GetString(2);
				this.TypeID = r.GetInt32(3);
				this.AccountId = r.IsDBNull(4) ? null : r.GetInt32(4);
				this.EmailHash = r.IsDBNull(5) ? null : r.GetString(5);
			}
		}

		public ITable<UserTypes_> UserTypes { get; }
		public IUniqueIndex<UserTypes_, int> UserTypes__Id { get; }
		public ITable<Users_> Users { get; }
		public IUniqueIndex<Users_, int> Users__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			UserTypes = Tables.Create<UserTypes_, int>(t => t.Id);
			UserTypes__Id = (IUniqueIndex<UserTypes_, int>)UserTypes.PrimaryKeyIndex;
			Users = Tables.Create<Users_, int>(t => t.Id);
			Users__Id = (IUniqueIndex<Users_, int>)Users.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.UserTypes.Insert(new DB.UserTypes_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2(IDataReader r) {
		while (r.Read()) {
			__db.Users.Insert(new DB.Users_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__3() {
		var result = new object[6];
		result[4] = DBNull.Value;
		result[5] = DBNull.Value;
		var __resultSet = from __users in __db.Users join __types in __db.UserTypes on __users.TypeID equals __types.Id select new { __users.Id, __users.Name, __users.Address, type = __types.Name };
		foreach (var __item in __resultSet) {
			result[0] = __item.Id;
			result[1] = __item.Name;
			result[2] = (object?)__item.Address ?? DBNull.Value;
			result[3] = __item.type;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""UserTypes"", Transformer__1);
		_streamDict.Add(""Users"", Transformer__2);
		_producers.Add((destDict.GetStream(""Users""), Transformer__3));
		_nameMap.Add(StreamDescription.Parse(""Orders""), StreamDescription.Parse(""OrderData""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}
";

		[Test]
		public void ParseCleanSpec()
		{
			var result = new Compiler().Compile("test", CLEAN_SPEC);
			Assert.That(result.Code, Is.EqualTo(FixDicts(CLEAN_OUTPUT)));
		}

		private const string FILTERED_INT = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream products2 as outDataDict.Products

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.Vendor = 1
into products2

sync myInput to myOutput";

		private const string FILTERED_INT_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[4];
		var __resultSet = from __products in __db.Products where __products.Vendor == 1 select new { __products.Id, name = __products.SKU, VendorID = __products.Vendor, __products.Price };
		foreach (var __item in __resultSet) {
			result[0] = __item.Id;
			result[1] = __item.name;
			result[2] = __item.VendorID;
			result[3] = __item.Price;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void ParseFilteredInt()
		{
			var result = new Compiler().Compile("test", FILTERED_INT);
			Assert.That(result.Code, Is.EqualTo(FixDicts(FILTERED_INT_OUTPUT)));
		}

		private const string FILTERED_STR = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream products2 as outDataDict.Products

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
where p.SKU = 'ABC123'
into products2

sync myInput to myOutput";

		private const string FILTERED_STR_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[4];
		var __resultSet = from __products in __db.Products where __products.SKU == ""ABC123"" select new { __products.Id, name = __products.SKU, VendorID = __products.Vendor, __products.Price };
		foreach (var __item in __resultSet) {
			result[0] = __item.Id;
			result[1] = __item.name;
			result[2] = __item.VendorID;
			result[3] = __item.Price;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void ParseFilteredStr()
		{
			var result = new Compiler().Compile("test", FILTERED_STR);
			Assert.That(result.Code, Is.EqualTo(FixDicts(FILTERED_STR_OUTPUT)));
		}

		private const string GROUPED = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream result as outDataDict.ProductMax

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.Vendor VendorID, max(p.Price)
from products p
group by Vendor
into result

sync myInput to myOutput";

		private const string GROUPED_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[2];
		var __preAgg = __db.Products;
		var aggregator__3 = Aggregates.Max<int, decimal>();
		foreach (var __item in __preAgg) {
			aggregator__3.Add(__item.Vendor, __item.Price);
		}
		foreach (var pair in aggregator__3) {
			result[0] = pair.Key;
			result[1] = pair.Value;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
		_nameMap.Add(StreamDescription.Parse(""Products""), StreamDescription.Parse(""ProductMax""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}
";

		[Test]
		public void ParseGrouped() 
		{
			var result = new Compiler().Compile("test", GROUPED);
			Assert.That(result.Code, Is.EqualTo(FixDicts(GROUPED_OUTPUT)));
		}

		private const string COUNTED = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream result as outDataDict.ProductCount

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.Vendor VendorID, count(*) Quantity
from products p
group by Vendor
into result

sync myInput to myOutput";

		private const string COUNTED_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[2];
		var __preAgg = __db.Products;
		var aggregator__3 = Aggregates.Count<int>();
		foreach (var __item in __preAgg) {
			aggregator__3.Add(__item.Vendor);
		}
		foreach (var pair in aggregator__3) {
			result[0] = pair.Key;
			result[1] = pair.Value;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
		_nameMap.Add(StreamDescription.Parse(""Products""), StreamDescription.Parse(""ProductCount""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}
";

		[Test]
		public void ParseCounted()
		{
			var result = new Compiler().Compile("test", COUNTED);
			Assert.That(result.Code, Is.EqualTo(FixDicts(COUNTED_OUTPUT)));
		}

		private const string HAVING = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream result as outDataDict.ProductCount

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.Vendor VendorID, count(*) Quantity
from products p
group by Vendor
having count(*) > 5
into result

sync myInput to myOutput";

		private const string HAVING_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[2];
		var __preAgg = __db.Products;
		var aggregator__3 = Aggregates.Count<int>();
		foreach (var __item in __preAgg) {
			aggregator__3.Add(__item.Vendor);
		}
		foreach (var pair in aggregator__3) {
			if (!(pair.Value > 5)) continue;
			result[0] = pair.Key;
			result[1] = pair.Value;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
		_nameMap.Add(StreamDescription.Parse(""Products""), StreamDescription.Parse(""ProductCount""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}
";

		[Test]
		public void ParseHaving()
		{
			var result = new Compiler().Compile("test", HAVING);
			Assert.That(result.Code, Is.EqualTo(FixDicts(HAVING_OUTPUT)));
		}

		private const string GROUPED2 = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream result as outDataDict.ProductMaxAndCount

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.Vendor VendorID, max(p.Price), count(p.Price) Quantity
from products p
group by Vendor
into result

sync myInput to myOutput";

		private const string GROUPED2_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[3];
		var __preAgg = __db.Products;
		var aggregator__3 = Aggregates.Max<int, decimal>();
		var aggregator__4 = Aggregates.Count<int>();
		foreach (var __item in __preAgg) {
			aggregator__3.Add(__item.Vendor, __item.Price);
			aggregator__4.Add(__item.Vendor);
		}
		foreach (var pair in Aggregates.Combine(aggregator__3, aggregator__4)) {
			result[0] = pair.Key;
			result[1] = pair.Value.Item2;
			result[2] = pair.Value.Item1;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
		_nameMap.Add(StreamDescription.Parse(""Products""), StreamDescription.Parse(""ProductMaxAndCount""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__5 = myInput.ReadFrom(myDataDict);
		reader__5 = new Sync(outDataDict).Transform(reader__5);
		await myOutput.Sync(reader__5, outDataDict);
	}
}
";

		[Test]
		public void ParseGrouped2()
		{
			var result = new Compiler().Compile("test", GROUPED2);
			Assert.That(result.Code, Is.EqualTo(FixDicts(GROUPED2_OUTPUT)));
		}

		private const string LITERAL1 = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream result as outDataDict.ProductMaxAndCount

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.Vendor VendorID, max(p.Price), 10 Quantity
from products p
group by Vendor
into result

sync myInput to myOutput";

		private const string LITERAL1_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[3];
		result[1] = 10;
		var __preAgg = __db.Products;
		var aggregator__3 = Aggregates.Max<int, decimal>();
		foreach (var __item in __preAgg) {
			aggregator__3.Add(__item.Vendor, __item.Price);
		}
		foreach (var pair in aggregator__3) {
			result[0] = pair.Key;
			result[2] = pair.Value;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
		_nameMap.Add(StreamDescription.Parse(""Products""), StreamDescription.Parse(""ProductMaxAndCount""));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__4 = myInput.ReadFrom(myDataDict);
		reader__4 = new Sync(outDataDict).Transform(reader__4);
		await myOutput.Sync(reader__4, outDataDict);
	}
}
"; 

		[Test]
		public void ParseLiteral1()
		{
			var result = new Compiler().Compile("test", LITERAL1);
			Assert.That(result.Code, Is.EqualTo(FixDicts(LITERAL1_OUTPUT)));
		}

		private const string LITERAL2 = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table users as myDataDict.Users
stream users2 as outDataDict.Users

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data from multiple sources.
  All JOINed tables must be declared as table, not stream.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select u.id, u.name, u.address, 'NONE' as type, null
from users u
into users2

sync myInput to myOutput
/*At EOF:
  Any streams not mentioned in input or output are auto-mapped to one another by equal names.
  All inputs and outputs are type-checked.  Errors are emitted as needed.
  Warnings are emitted for unused streams
  input streams declared as Table are not auto-mapped.  If manually mapped, transfer will work as expected
  Run codegen */";

		private const string LITERAL2_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Users_ {
			public int Id { get; }
			public string Name { get; }
			public string? Address { get; }
			public int TypeID { get; }
			public int? AccountId { get; }
			public string? EmailHash { get; }

			public Users_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.Name = r.GetString(1);
				this.Address = r.IsDBNull(2) ? null : r.GetString(2);
				this.TypeID = r.GetInt32(3);
				this.AccountId = r.IsDBNull(4) ? null : r.GetInt32(4);
				this.EmailHash = r.IsDBNull(5) ? null : r.GetString(5);
			}
		}

		public ITable<Users_> Users { get; }
		public IUniqueIndex<Users_, int> Users__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Users = Tables.Create<Users_, int>(t => t.Id);
			Users__Id = (IUniqueIndex<Users_, int>)Users.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Users.Insert(new DB.Users_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[6];
		result[3] = ""NONE"";
		result[4] = DBNull.Value;
		result[5] = DBNull.Value;
		var __resultSet = __db.Users;
		foreach (var __item in __resultSet) {
			result[0] = __item.Id;
			result[1] = __item.Name;
			result[2] = (object?)__item.Address ?? DBNull.Value;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Users"", Transformer__1);
		_producers.Add((destDict.GetStream(""Users""), Transformer__2));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void ParseLiteral2()
		{
			var result = new Compiler().Compile("test", LITERAL2);
			Assert.That(result.Code, Is.EqualTo(FixDicts(LITERAL2_OUTPUT)));
		}

		private const string ORDERED = @"
load myDataDict from '.\myDataDict.pansync' --loads the file into a variable named myDataDict
load outDataDict from '.\outDataDict.pansync' --loads the file into a variable named outDataDict

table products as myDataDict.Products
stream products2 as outDataDict.Products

--opens a reader of type MsSql with the provided connection string, associated with myDataDict
open myInput as MsSql for read with myDataDict, 'connection string here'

--opens a writer of type Postgres with a stored connection string retrieved from an external source, associated with outDataDict
open myOutput as Postgres for write with outDataDict, CredentialsFromEnv('PostgresConnectionString')

/*Uses a SQL query to process data, filtering it by vendor.
  Must end with an ""into"" clause referencing a previously-defined table or stream.
  Output is type-checked, and will error if it doesn't match the defined table.
*/
select p.id, p.SKU as name, p.Vendor VendorID, p.Price
from products p
order by p.Vendor
into products2

sync myInput to myOutput";

		private const string ORDERED_OUTPUT = @"using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Core;
using static Pansynchro.PanSQL.Core.Credentials;

using NMemory;
using NMemory.Indexes;
using NMemory.Tables;

class Sync : StreamTransformerBase {
	private class DB : Database {
		public class Products_ {
			public int Id { get; }
			public string SKU { get; }
			public int Vendor { get; }
			public decimal Price { get; }

			public Products_(IDataReader r) {
				this.Id = r.GetInt32(0);
				this.SKU = r.GetString(1);
				this.Vendor = r.GetInt32(2);
				this.Price = r.GetDecimal(3);
			}
		}

		public ITable<Products_> Products { get; }
		public IUniqueIndex<Products_, int> Products__Id { get; }

		public DB() {
			NMemory.NMemoryManager.DisableObjectCloning = true;
			Products = Tables.Create<Products_, int>(t => t.Id);
			Products__Id = (IUniqueIndex<Products_, int>)Products.PrimaryKeyIndex;
		}
	}

	private readonly DB __db = new();
	private readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)> _producers = new();

	private IEnumerable<object?[]> Transformer__1(IDataReader r) {
		while (r.Read()) {
			__db.Products.Insert(new DB.Products_(r));
		}
		yield break;
	}

	private IEnumerable<object?[]> Transformer__2() {
		var result = new object[4];
		var __resultSet = from __products in __db.Products orderby __products.Vendor select new { __products.Id, name = __products.SKU, VendorID = __products.Vendor, __products.Price };
		foreach (var __item in __resultSet) {
			result[0] = __item.Id;
			result[1] = __item.name;
			result[2] = __item.VendorID;
			result[3] = __item.Price;
			yield return result;
		}
	}

	public override async IAsyncEnumerable<DataStream> StreamLast() {
		foreach (var (stream, producer) in _producers) {
			yield return new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream));
		}
		await Task.CompletedTask;
	}

	public Sync(DataDictionary destDict) : base(destDict) {
		_streamDict.Add(""Products"", Transformer__1);
		_producers.Add((destDict.GetStream(""Products""), Transformer__2));
	}
}

static class Program {
	public static async Task Main() {
		var myDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$INDICT$""));
		var outDataDict = DataDictionaryWriter.Parse(CompressionHelper.Decompress(""$OUTDICT$""));
		var myInput = ConnectorRegistry.GetReader(""MSSQL"", ""connection string here"");
		var myOutput = ConnectorRegistry.GetWriter(""Postgres"", CredentialsFromEnv(""PostgresConnectionString""));
		var reader__3 = myInput.ReadFrom(myDataDict);
		reader__3 = new Sync(outDataDict).Transform(reader__3);
		await myOutput.Sync(reader__3, outDataDict);
	}
}
";

		[Test]
		public void AllowOrdering()
		{
			var result = new Compiler().Compile("test", ORDERED);
			Assert.That(result.Code, Is.EqualTo(FixDicts(ORDERED_OUTPUT)));
		}
	}
}