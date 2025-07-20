using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Parquet.Data;
using Parquet.Schema;
using PReader = Parquet.ParquetReader;
using SchemaType = Parquet.Schema.SchemaType;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Connectors.Parquet
{

	public class ParquetAnalyzer : ISchemaAnalyzer, ISourcedConnector
	{
		private IDataSource? _source;

		public ParquetAnalyzer(string config)
		{ }

		public async ValueTask<DataDictionary> AnalyzeAsync(string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
			}
			string? lastName = null;
			var defs = new List<StreamDefinition>();
			await foreach (var (sName, stream) in _source.GetDataAsync()) {
				if (lastName != sName) {
					defs.Add(await AnalyzeFile(sName, stream));
					lastName = sName;
				} else {
					stream.Dispose();
				}
			}
			return new DataDictionary(name, defs.ToArray());
		}

		private static async Task<StreamDefinition> AnalyzeFile(string name, Stream stream)
		{
			using var lStream = StreamHelper.SeekableStream(stream);
			using var reader = await PReader.CreateAsync(lStream);
			var fields = reader.Schema.GetDataFields().Select(AnalyzeField).ToArray();
			return new StreamDefinition(new StreamDescription(null, name), fields, Array.Empty<string>());
		}

		private static FieldDefinition AnalyzeField(DataField field)
		{
			TypeTag type;
			if (field.SchemaType == SchemaType.Data) {
				if (field.ClrType == typeof(bool)) type = TypeTag.Boolean;
				else if (field.ClrType == typeof(byte)) type = TypeTag.Byte;
				else if (field.ClrType == typeof(sbyte)) type = TypeTag.SByte;
				else if (field.ClrType == typeof(short)) type = TypeTag.Short;
				else if (field.ClrType == typeof(ushort)) type = TypeTag.UShort;
				else if (field.ClrType == typeof(int)) type = TypeTag.Int;
				else if (field.ClrType == typeof(uint)) type = TypeTag.UInt;
				else if (field.ClrType == typeof(long)) type = TypeTag.Long;
				else if (field.ClrType == typeof(ulong)) type = TypeTag.ULong;
				else if (field.ClrType == typeof(byte[])) type = TypeTag.Blob;
				else if (field.ClrType == typeof(string)) type = TypeTag.Ntext;
				else if (field.ClrType == typeof(float)) type = TypeTag.Single;
				else if (field.ClrType == typeof(double)) type = TypeTag.Double;
				else if (field.ClrType == typeof(decimal)) type = TypeTag.Decimal;
				else if (field.ClrType == typeof(DateTime)) type = TypeTag.DateTime;
				else if (field.ClrType == typeof(DateTimeOffset)) type = TypeTag.DateTimeTZ;
				else if (field.ClrType == typeof(TimeSpan)) type = TypeTag.Interval;
				else throw new NotSupportedException($"Parquet field '{field.Name}' is not of a supported type");
			} else throw new NotSupportedException($"Parquet field '{field.Name}' is of a {field.SchemaType} type, which is not currently supported");
			IFieldType fType = new BasicField(type, field.IsNullable, null, false);
			if (field.IsArray) {
				fType = new CollectionField(fType, CollectionType.Array, false);
			}
			return new FieldDefinition(field.Name, fType);
		}

		public void SetDataSource(IDataSource source) => _source = source;
	}
}