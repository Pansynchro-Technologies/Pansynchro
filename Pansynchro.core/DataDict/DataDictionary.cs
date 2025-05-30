using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Core.DataDict
{
	public record DataDictionary(
		string Name,
		StreamDefinition[] Streams,
		StreamDescription[][] DependencyOrder,
		Dictionary<string, IFieldType> CustomTypes)
	{
		public DataDictionary(string name, StreamDefinition[] streams)
			: this(name, streams, Array.Empty<StreamDescription[]>(), new()) { }

		public Dictionary<string, string[]> Names => Streams.ToDictionary(s => s.Name.ToString(), s => s.NameList, StringComparer.InvariantCultureIgnoreCase);

		private Dictionary<string, StreamDefinition>? _streamDict;

		private void EnsureStreamDict()
		{
			if (_streamDict == null) {
				_streamDict = Streams.ToDictionary(s => s.Name.ToString(), StringComparer.InvariantCultureIgnoreCase);
			}
		}

		public StreamDefinition GetStream(string name)
		{
			EnsureStreamDict();
			return _streamDict![name];
		}

		public StreamDefinition GetStream(StreamDescription name, NameStrategy strategy)
		{
			EnsureStreamDict();
			var mName = strategy.MatchingName(name);
			if (_streamDict!.TryGetValue(mName, out var result)) {
				return result;
			}
			return Streams.First(s => strategy.MatchingName(s.Name) == mName);
		}

		public bool HasStream(string name)
		{
			EnsureStreamDict();
			return _streamDict!.ContainsKey(name);
		}

		public static DataDictionary LoadFromFile(string filename) 
			=> DataDictionaryWriter.Parse(File.ReadAllText(filename));

		public void SaveToFile(string filename) => File.WriteAllText(filename, DataDictionaryWriter.Write(this));

		public override string ToString() => DataDictionaryWriter.Write(this);

		// HACK: Exclude _streamDict from the WITH operator's cloning process.
		// Will need to be manually updated if new fields are added to DataDictionary.
		// 
		protected DataDictionary(DataDictionary original)
		{
			this.Name = original.Name;
			this.Streams = original.Streams;
			this.DependencyOrder = original.DependencyOrder;
			this.CustomTypes = original.CustomTypes;
		}
	}

	public record StreamDefinition(StreamDescription Name, FieldDefinition[] Fields, string[] Identity, string? CustomQuery = null)
	{
		public string[] NameList => Fields.Select(f => f.Name).ToArray();
		public string[] RareChangeFields { get; set; } = Array.Empty<string>();
		public KeyValuePair<string, long>[] DomainReductions { get; set; } = Array.Empty<KeyValuePair<string, long>>();
		public int? SeqIdIndex { get; set; }
		public int? AuditFieldIndex { get; set; }

		public StreamDefinition WithOptimizations(
			string[] rcfFields, KeyValuePair<string, long>[] domainShifts, int? seqId)
		{
			var fixedFields = Fields.Where(f => !rcfFields.Contains(f.Name))
				.Concat(rcfFields.Select(n => Fields.First(f => f.Name == n)))
				.ToArray();
			return this with { Fields = fixedFields, RareChangeFields = rcfFields, DomainReductions = domainShifts, SeqIdIndex = seqId };
		}

		bool IEquatable<StreamDefinition>.Equals(StreamDefinition? other) 
			=> other != null 
				&& Name == other.Name
				&& Fields.SequenceEqual(other.Fields)
				&& Identity.SequenceEqual(other.Identity)
				&& CustomQuery == other.CustomQuery
				&& RareChangeFields.SequenceEqual(other.RareChangeFields)
				&& DomainReductions.SequenceEqual(other.DomainReductions)
				&& SeqIdIndex == other.SeqIdIndex
				&& AuditFieldIndex == other.AuditFieldIndex;
	}

	public record FieldDefinition(string Name, IFieldType Type, string? CustomRead = null);

	/*
	public record FieldType(TypeTag Type, bool Nullable, CollectionType CollectionType, string? Info)
	{
		// for data with a high degree of entropy, such as random numbers, non-sequential GUIDs,
		// or compressed binary data.  Attempting to compress this is a waste of time.
		public bool Incompressible { get; set; }

		public bool CanAssignNotNullToNull(in FieldType dest) => (!Nullable) && dest.Nullable
				&& Type == dest.Type
				&& Info == dest.Info
				&& CollectionType == dest.CollectionType;

		public bool CanAssignSpecificToGeneral(in FieldType dest) =>
			((Nullable == dest.Nullable) || ((!Nullable) && dest.Nullable))
				&& Type == dest.Type
				&& (Info != null && dest.Info == null)
				&& CollectionType == dest.CollectionType;

		public override string ToString()
		{
			var result = Type.ToString();
			if (Info != null) {
				result += $"({Info})";
			}
			if (Nullable) {
				result += " NULL";
			}
			if (CollectionType != CollectionType.None) {
				result = $"{CollectionType}[{result}]";
			}
			return result;
		}
	}
	*/

	public enum CollectionType
	{
		Array,
		Multiset
	}

	public enum TypeTag
	{
		None, // represents NULL literals
		Unstructured,
		Char,
		Varchar,
		Text,
		Nchar,
		Nvarchar,
		Ntext,
		Binary,
		Varbinary,
		Blob,
		Boolean,
		Byte,
		Short,
		Int,
		Long,
		Decimal,
		Numeric,
		Float,
		Single,
		Double,
		Date,
		Time,
		TimeTZ,
		DateTime,
		DateTimeTZ,
		VarDateTime,
		Guid,
		Interval,
		Xml,
		Json,
		Geography,
		Geometry,
		HierarchyID,
		Money,
		SmallMoney,
		SmallDateTime,
		Bits,
		Decimal64,
		Decimal128,
		Int128,
		SByte,
		UShort,
		UInt,
		ULong,
	}
}
