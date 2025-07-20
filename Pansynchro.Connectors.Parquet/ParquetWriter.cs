using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Parquet;
using Parquet.Data;
using PWriter = Parquet.ParquetWriter;
using PSchema = Parquet.Schema.ParquetSchema;
using DataColumn = Parquet.Data.DataColumn;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;
using Pansynchro.Core.DataDict.TypeSystem;
using Parquet.Schema;

namespace Pansynchro.Connectors.Parquet
{
	public class ParquetWriter : IWriter, ISinkConnector
	{
		private IDataSink? _sink;

		public void SetDataSink(IDataSink sink)
		{
			_sink = sink;
		}

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			if (_sink == null) {
				throw new DataException("Must call SetDataSink before calling Sync");
			}
			EventLog.Instance.AddStartSyncEvent();
			await foreach (var (name, settings, reader) in streams) {
				EventLog.Instance.AddStartSyncStreamEvent(name);
				try {
					var streamDef = dest.GetStream(name, NameStrategy.Get(NameStrategyType.Identity));
					var (schema, writers) = ParquetWriter.BuildSchema(streamDef);
					using var writer = await PWriter.CreateAsync(schema, await _sink.WriteData(name.ToString()));
					using var group = writer.CreateRowGroup();
					await ParquetWriter.WriteParquetData(reader, writers, group);
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, name);
					if (!ErrorManager.ContinueOnError)
						throw;
				} finally {
					reader.Dispose();
				}
				EventLog.Instance.AddEndSyncStreamEvent(name);
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		private static async Task WriteParquetData(
			IDataReader reader, Func<object[][], DataColumn>[] writers, ParquetRowGroupWriter group)
		{
			var len = writers.Length;
			var list = new List<object[]>();
			while (reader.Read()) {
				var buffer = new object[len];
				reader.GetValues(buffer);
				list.Add(buffer);
			}
			var grid = list.ToArray();
			foreach (var writer in writers) {
				var column = writer(grid);
				await group.WriteColumnAsync(column);
			}
		}

		private static (PSchema schema, Func<object[][], DataColumn>[] writers) BuildSchema(
			StreamDefinition streamDef)
		{
			var fields = new List<Field>();
			var writers = new List<Func<object[][], DataColumn>>();
			for (int i = 0; i < streamDef.Fields.Length; ++i) {
				var field = BuildField(streamDef.Fields[i]);
				fields.Add(field);
				writers.Add(ParquetWriter.BuildWriter(streamDef.Fields[i].Type, field, i));
			}
			var schema = new PSchema(fields);
			return (schema, writers.ToArray());
		}

		private static Func<object[][], DataColumn> BuildWriter(IFieldType ft, DataField field, int idx)
		{
			var arrayBuilder = MakeArrayBuilder(ft, idx);
			return grid => new DataColumn(field, arrayBuilder(grid));
		}

		private static Func<object[][], Array> MakeArrayBuilder(IFieldType ft, int idx)
		{
			var nullable = ft.Nullable;
			var bt = ft as BasicField ?? throw new NotSupportedException("Arrays of non-primitive types are not supported");
			return bt.Type switch {
				TypeTag.Boolean => nullable ? DoMakeNullableArrayBuilder<bool>(idx) : DoMakeArrayBuilder<bool>(idx),
				TypeTag.Byte => nullable ? DoMakeNullableArrayBuilder<byte>(idx) : DoMakeArrayBuilder<byte>(idx),
				TypeTag.SByte => nullable ? DoMakeNullableArrayBuilder<sbyte>(idx) : DoMakeArrayBuilder<sbyte>(idx),
				TypeTag.Short => nullable ? DoMakeNullableArrayBuilder<short>(idx) : DoMakeArrayBuilder<short>(idx),
				TypeTag.UShort => nullable ? DoMakeNullableArrayBuilder<ushort>(idx) : DoMakeArrayBuilder<ushort>(idx),
				TypeTag.Int => nullable ? DoMakeNullableArrayBuilder<int>(idx) : DoMakeArrayBuilder<int>(idx),
				TypeTag.UInt => nullable ? DoMakeNullableArrayBuilder<uint>(idx) : DoMakeArrayBuilder<uint>(idx),
				TypeTag.Long => nullable ? DoMakeNullableArrayBuilder<long>(idx) : DoMakeArrayBuilder<long>(idx),
				TypeTag.ULong => nullable ? DoMakeNullableArrayBuilder<ulong>(idx) : DoMakeArrayBuilder<ulong>(idx),
				TypeTag.Blob or TypeTag.Binary or TypeTag.Varbinary =>
					nullable ? DoMakeNullableArrayRefBuilder<byte[]>(idx) : DoMakeArrayBuilder<byte[]>(idx),
				TypeTag.Ntext or TypeTag.Text or TypeTag.Varchar or TypeTag.Nvarchar or TypeTag.Char =>
					nullable ? DoMakeNullableArrayRefBuilder<string>(idx) : DoMakeArrayBuilder<string>(idx),
				TypeTag.Single or TypeTag.Float =>
					nullable ? DoMakeNullableArrayBuilder<float>(idx) : DoMakeArrayBuilder<float>(idx),
				TypeTag.Double => nullable ? DoMakeNullableArrayBuilder<double>(idx) : DoMakeArrayBuilder<double>(idx),
				TypeTag.Decimal => nullable ? DoMakeNullableArrayBuilder<decimal>(idx) : DoMakeArrayBuilder<decimal>(idx),
				TypeTag.DateTimeTZ => nullable ? DoMakeNullableArrayBuilder<DateTimeOffset>(idx) : DoMakeArrayBuilder<DateTimeOffset>(idx),
				TypeTag.DateTime => nullable ? DoMakeNullableDateTimeBuilder(idx) : DoMakeDateTimeBuilder(idx),
				TypeTag.Interval => nullable ? DoMakeNullableArrayBuilder<TimeSpan>(idx) : DoMakeArrayBuilder<TimeSpan>(idx),
				_ => throw new NotSupportedException($"Field data type '{bt.Type}' is not supported")
			};
		}

		private static Func<object[][], Array> DoMakeArrayBuilder<T>(int idx)
		{
			return grid => {
				var result = new T[grid.Length];
				for (int i = 0; i < grid.Length; ++i) {
					result[i] = (T)grid[i][idx];
				}
				return result;
			};
		}

		private static Func<object[][], Array> DoMakeNullableArrayBuilder<T>(int idx) where T : struct
		{
			return grid => {
				var result = new T?[grid.Length];
				for (int i = 0; i < grid.Length; ++i) {
					var value = grid[i][idx];
					result[i] = value == null || value == DBNull.Value ? (T?)null : (T)value;
				}
				return result;
			};
		}

		private static Func<object[][], Array> DoMakeDateTimeBuilder(int idx)
		{
			return grid => {
				var result = new DateTimeOffset[grid.Length];
				for (int i = 0; i < grid.Length; ++i) {
					result[i] = new DateTimeOffset((DateTime)grid[i][idx]);
				}
				return result;
			};
		}

		private static Func<object[][], Array> DoMakeNullableArrayRefBuilder<T>(int idx) where T : class
		{
			return grid => {
				var result = new T?[grid.Length];
				for (int i = 0; i < grid.Length; ++i) {
					var value = grid[i][idx];
					result[i] = value == null || value == DBNull.Value ? null : (T)value;
				}
				return result;
			};
		}

		private static Func<object[][], Array> DoMakeNullableDateTimeBuilder(int idx)
		{
			return grid => {
				var result = new DateTimeOffset?[grid.Length];
				for (int i = 0; i < grid.Length; ++i) {
					var value = grid[i][idx];
					result[i] = value == null || value == DBNull.Value ? (DateTimeOffset?)null : new DateTimeOffset((DateTime)value);
				}
				return result;
			};
		}

		private static DataField BuildField(FieldDefinition fd)
		{
			var type = fd.Type as BasicField ?? throw new DataException("Non-primitive types are not supported yet");
			return new DataField(fd.Name, LookupDataType(type.Type), type.Nullable);
		}

		private static Type LookupDataType(TypeTag type)
			=> type switch {
				TypeTag.Boolean => typeof(bool),
				TypeTag.Byte => typeof(byte),
				TypeTag.SByte => typeof(sbyte),
				TypeTag.Short => typeof(short),
				TypeTag.UShort => typeof(ushort),
				TypeTag.Int => typeof(int),
				TypeTag.UInt => typeof(uint),
				TypeTag.Long => typeof(long),
				TypeTag.ULong => typeof(ulong),
				TypeTag.Blob or TypeTag.Binary or TypeTag.Varbinary => typeof(byte[]),
				TypeTag.Ntext or TypeTag.Text or TypeTag.Varchar or TypeTag.Nvarchar or TypeTag.Char => typeof(string),
				TypeTag.Single or TypeTag.Float => typeof(float),
				TypeTag.Double => typeof(double),
				TypeTag.Decimal => typeof(decimal),
				TypeTag.DateTimeTZ or TypeTag.DateTime => typeof(DateTimeOffset),
				TypeTag.Interval => typeof(TimeSpan),
				_ => throw new NotSupportedException($"Field data type '{type}' is not supported")
			};

		public void Dispose()
		{
			(_sink as IDisposable)?.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
