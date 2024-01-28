using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.TextFile.CSV
{
	internal class CsvDataReader : ArrayReader
	{
		private CsvArrayProducer _producer;
		private int _count;

		public CsvDataReader(CsvArrayProducer producer)
		{
			_producer = producer;
			_buffer = new object[producer.Names.Length];
		}

		public override int RecordsAffected => _count;

		public override string GetName(int i) => _producer.Names[i];

		public override bool Read()
		{
			if (_producer.Produce(_buffer)) {
				++_count;
				return true;
			}
			return false;
		}

		public override void Dispose()
		{
			_producer.Dispose();
			GC.SuppressFinalize(this);
		}
	}

	internal class CsvArrayProducer : IDisposable
	{
		private const int ANALYZE_LINES = 10;

		private readonly TextReader _reader;
		private readonly IEnumerator<string> _lineReader;
		private (char Delimiter, char QuoteChar, char EscapeChar, bool Trim, bool UsesQuotes, bool EolInData, bool DecimalAsSingle) _config;
		private readonly string[] _analyzeBuffer;
		private int _index = 0;

		public (Type type, bool nullable)[] FieldTypes { get; }

#if NET8_0_OR_GREATER
		delegate void SpanReader(ReadOnlySpan<char> input, object[] buffer);
		private readonly SpanReader[] _fieldReader;
#else
		private readonly Action<string, object[]>[] _fieldReader;
#endif

		protected int FieldCount => _fieldReader.Length;

		public string[] Names { get; }

		public CsvArrayProducer(TextReader reader, CsvConfigurator config)
		{
			_reader = reader;
			_config = (config.Delimiter, config.QuoteChar, config.EscapeChar, config.Trim, config.UsesQuotes, config.EolInData, config.DecimalAsSingle);
			_lineReader = ReadLines().GetEnumerator();
			Names = config.UsesHeader ? ReadHeader() : null!;
			var buffer = new List<string>(ANALYZE_LINES);
			for (int i = 0; i < ANALYZE_LINES; i++) {
				if (!_lineReader.MoveNext()) {
					break;
				}
				buffer.Add(_lineReader.Current);
			}
			_analyzeBuffer = buffer.ToArray();
			FieldTypes = AnalyzeTypes();
			_fieldReader = BuildFieldReaders(FieldTypes);
			if (!config.UsesHeader) {
				Names = Enumerable.Range(1, FieldTypes.Length).Select(i => $"Item{i}").ToArray();
			}
		}

#if NET8_0_OR_GREATER
		private static SpanReader[] BuildFieldReaders((Type type, bool nullable)[] fieldTypes)
		{
			var result = new SpanReader[fieldTypes.Length];
			for (int i = 0; i < fieldTypes.Length; ++i) {
				var reader = BuildFieldReader(fieldTypes[i].type, fieldTypes[i].nullable, i);
				result[i] = reader;
			}
			return result;
		}

		private static SpanReader BuildFieldReader(Type type, bool nullable, int i)
		{
			SpanReader result;
			if (type == typeof(bool)) {
				result = (value, r) => r[i] = bool.Parse(value);
			} else if (type == typeof(long)) {
				result = (value, r) => r[i] = long.Parse(value);
			} else if (type == typeof(double)) {
				result = (value, r) => r[i] = double.Parse(value);
			} else if (type == typeof(float)) {
				result = (value, r) => r[i] = float.Parse(value);
			} else if (type == typeof(decimal)) {
				result = (value, r) => r[i] = decimal.Parse(value);
			} else if (type == typeof(DateTime)) {
				result = (value, r) => r[i] = DateTime.Parse(value);
			} else if (type == typeof(Guid)) {
				result = (value, r) => r[i] = Guid.Parse(value);
			} else if (type == typeof(char)) {
				result = (value, r) => r[i] = value[0];
			} else if (type == typeof(string)) {
				return (value, r) => r[i] = value.IsEmpty ? string.Empty : new string(value);
			} else {
				throw new NotImplementedException();
			}
			if (nullable) {
				return (value, r) => { if (value.IsEmpty) { r[i] = DBNull.Value; } else result(value, r); };
			}
			return (value, r) => { if (value.IsEmpty) { throw new InvalidDataException("Null value in non-nullable column"); } else result(value, r); };
		}
#else
		private static Action<string, object[]>[] BuildFieldReaders((Type type, bool nullable)[] fieldTypes)
		{
			var result = new Action<string, object[]>[fieldTypes.Length];
			for (int i = 0; i < fieldTypes.Length; ++i) {
				var reader = BuildFieldReader(fieldTypes[i].type, fieldTypes[i].nullable, i);
				result[i] = reader;
			}
			return result;
		}

		private static Action<string, object[]> BuildFieldReader(Type type, bool nullable, int i)
		{
			Action<string, object[]> result;
			if (type == typeof(bool)) {
				result = (value, r) => r[i] = bool.Parse(value);
			} else if (type == typeof(long)) {
				result = (value, r) => r[i] = long.Parse(value);
			} else if (type == typeof(double)) {
				result = (value, r) => r[i] = double.Parse(value);
			} else if (type == typeof(decimal)) {
				result = (value, r) => r[i] = decimal.Parse(value);
			} else if (type == typeof(DateTime)) {
				result = (value, r) => r[i] = DateTime.Parse(value);
			} else if (type == typeof(Guid)) {
				result = (value, r) => r[i] = Guid.Parse(value);
			} else if (type == typeof(char)) {
				result = (value, r) => r[i] = value[0];
			} else if (type == typeof(string)) {
				return (value, r) => r[i] = string.IsNullOrEmpty(value) ? string.Empty : value;
			} else {
				throw new NotImplementedException();
			}
			if (nullable) {
				return (value, r) => { if (string.IsNullOrEmpty(value)) { r[i] = DBNull.Value; } else result(value, r); };
			}
			return (value, r) => { if (string.IsNullOrEmpty(value)) { throw new InvalidDataException("Null value in non-nullable column"); } else result(value, r); };
		}
#endif

		private string[] ReadHeader() => _lineReader.MoveNext() ? GetFieldValues(_lineReader.Current) : Array.Empty<string>();

		private (Type, bool)[] AnalyzeTypes()
		{
			Type?[]? types = null;
			bool[]? hasNulls = null!;
			for (int i = 0; i < _analyzeBuffer.Length; ++i) {
				var values = GetFieldValues(_analyzeBuffer[i]);
				types ??= new Type[values.Length];
				hasNulls ??= new bool[values.Length];
				for (int j = 0; j < values.Length; ++j) {
					types[j] = DetermineType(types[j], values[j], i == _analyzeBuffer.Length - 1, ref hasNulls[j]);
				}
			}
			return types!.Zip(hasNulls, (t, b) => (t!, b)).ToArray();
		}

		private string[] GetFieldValues(string line)
		{
			var splitOptions = _config.Trim ? StringSplitOptions.TrimEntries : StringSplitOptions.None;
			if (_config.UsesQuotes) {
				return Split(line, _config.Delimiter, splitOptions, _config.QuoteChar, _config.EscapeChar, mayContainEOLInData: _config.EolInData);
			} else if (_config.EolInData) {
				List<string> fvs = new List<string>();
				int pos = 0;
				string? remaining = line;
				while (remaining != null) {
					var tokens = Split(remaining, _config.Delimiter, splitOptions, _config.QuoteChar, _config.EscapeChar, mayContainEOLInData: true);
					if (tokens.Length > 0) {
						var fv = tokens.First();
						fvs.Add(fv);
						if (remaining.Contains(fv + _config.Delimiter, StringComparison.CurrentCulture))
							remaining = RightOf(remaining, fv + _config.Delimiter);
						else
							remaining = null;
					} else {
						fvs.Add(String.Empty);
						remaining = null;
					}
					pos++;
				}
				return fvs.ToArray();
			} else {
				return line.Split(_config.Delimiter, splitOptions); //in the simplest case, keep it simple!
			}
		}

		private static string? NormalizeString(string? inString, char quoteChar)
		{
			inString = inString?.Trim();
			if (inString == null || inString.Length < 2) return inString;
			if (inString[0] == quoteChar && inString[inString.Length - 1] == quoteChar)
				return inString.Substring(1, inString.Length - 2);
			else
				return inString;
		}

		public static string[] FastSplit(string line, char? cSeparator = ',', char? cQuotes = '"', char? cQuoteEscape = '\\')
		{
			char escapeChar = cQuoteEscape == null ? '\\' : cQuoteEscape.Value;
			List<string> result = new List<string>();
			StringBuilder currentStr = new StringBuilder("");
			bool inQuotes = false;
			int length = line.Length;
			bool lineEnded = false;

			for (int i = 0; i < line.Length; i++) // For each character
			{
				if (!inQuotes && line[i] == escapeChar && i + 1 < length && line[i + 1] == escapeChar)
				{
					currentStr.Append(line[i + 1]);
					i++;
				}
				else if (!inQuotes && line[i] == escapeChar && i + 1 < length && line[i + 1] == cQuotes)
				{
					currentStr.Append(line[i + 1]);
					i++;
				}
				else if (!inQuotes && line[i] == cQuotes)
				{
					//if (i == length - 1)
					currentStr.Append(line[i]);
					inQuotes = !inQuotes;
				}
				else if (inQuotes &&
					(
						((i + 1 < length && line[i + 1] == cSeparator) || (i + 1 == length))
					)
					) // Comma
				{
					if (line[i] == cQuotes) // If not in quotes, end of current string, add it to result
					{
						currentStr.Append(line[i]);
						inQuotes = false;
						lineEnded = i + 1 == length;
						result.Add(currentStr.ToString());
						currentStr.Clear();
						i++;
					}
					else
						currentStr.Append(line[i]); // If in quotes, just add it 
				}
				else if (inQuotes && line[i] == escapeChar && i + 1 < length && line[i + 1] == escapeChar)
				{
					currentStr.Append(line[i + 1]);
					i++;
				}
				else if (inQuotes &&
					   ((line[i] == escapeChar && i + 1 < length && line[i + 1] == cQuotes))
				   ) // Comma
				{
					i++;
					//inQuotes = false;
					currentStr.Append(line[i]);
				}
				else if (inQuotes && line[i] == cQuotes)
				{
					if (i + 1 < length && line[i + 1] == cQuotes)
					{
						currentStr.Append(line[i]);
						i++;
					}
					else
					{
						if (i + 1 < length && line[i + 1] != cSeparator)
						{
							currentStr.Append(line[i]);
						}
						//else
						inQuotes = false;
					}
				}
				else if (line[i] == cSeparator) // Comma
				{
					if (!inQuotes) // If not in quotes, end of current string, add it to result
					{
						var value = currentStr.ToString();
						if (value.Length == 1 && value[0] == cQuoteEscape)
							result.Add(String.Empty);
						else
							result.Add(value);
						currentStr.Clear();
					}
					else
						currentStr.Append(line[i]); // If in quotes, just add it 
				}
				else // Add any other character to current string
					currentStr.Append(line[i]);
			}
			if (!lineEnded)
			{
				result.Add(currentStr.ToString());
			}
			return result.ToArray(); // Return array of all strings
		}

		private static string[] Split(string text, char separator, StringSplitOptions stringSplitOptions,
			char quoteChar = '\0', char quoteEscape = '\\', bool mayContainEOLInData = false, bool allowQuotes = false)
		{
			if (String.IsNullOrEmpty(text)) return Array.Empty<string>();

			if (!mayContainEOLInData) {
				if (quoteChar == '\0') {
					return text.Split(separator);
				}
				return FastSplit(text, separator, quoteChar, quoteEscape);
			}

			List<string> splitStrings = new List<string>();

			if (separator == quoteChar) {
				throw new ApplicationException("Invalid separator characters passed.");
			}

			int len = 0;
			int i = 0;
			int quotes = 0;
			int singleQuotes = 0;
			int offset = 0;
			bool hasChar = false;
			string? word = null;
			while (i < text.Length) {
				if ((!allowQuotes) && text[i] == quoteChar) {
					quotes++;
				} else if (text[i] == '\\') {
					i++;
				} else if ((text[i] == separator) &&
					((quotes > 0 && quotes % 2 == 0) || (singleQuotes > 0 && singleQuotes % 2 == 0))
					|| text[i] == separator && quotes == 0 && singleQuotes == 0)
				{
					if (hasChar) {
						string subString = offset == 0 ? text.Substring(offset, i) : text.Substring(offset, i - offset);
						word = NormalizeString(subString.Replace("\\", String.Empty), quoteChar);
						splitStrings.Add(word);
						hasChar = false;
					} else {
						string subString = offset == 0 ? text.Substring(offset, i) : text.Substring(offset, i - offset);
						word = NormalizeString(subString, quoteChar);
						splitStrings.Add(word);
						i = i + len;
					}
					offset = i + 1;
				}
				i++;
			}

			if (offset <= text.Length)
				splitStrings.Add(hasChar 
					? NormalizeString(text.Substring(offset).Replace("\\", String.Empty), quoteChar)
					: NormalizeString(text.Substring(offset), quoteChar));

			return splitStrings.ToArray();
		}

		private static string? RightOf(string? source, string searchText)
		{
			if (source == null)
				return source;

			if (string.IsNullOrEmpty(searchText))
				throw new ArgumentException("Invalid searchText passed.");

			int index = source.IndexOf(searchText);
			if (index < 0)
				return String.Empty;
			index = index + searchText.Length;
			return source.Substring(index);
		}

		protected Type? DetermineType(Type? existingType, string value, bool isLastScanRow, ref bool hasNulls)
		{
			if (value == "" && existingType != null && existingType != typeof(string)) {
				hasNulls = true;
				return existingType;
			}
			var ci = Thread.CurrentThread.CurrentCulture;
			Type? fieldType;

			if (bool.TryParse(value.ToString(), out _))
				fieldType = typeof(bool);
			else if (Guid.TryParse(value.ToString(), out _))
				fieldType = typeof(Guid);
			else if (!(value.ToString().Contains(ci.NumberFormat.NumberDecimalSeparator)) && long.TryParse(value.ToString(), out _))
				fieldType = typeof(long);
			else if (double.TryParse(value.ToString(), out _))
				fieldType = _config.DecimalAsSingle ? typeof(float) : typeof(double);
			else if (decimal.TryParse(value.ToString(), out _))
				fieldType = typeof(decimal);
			else if (Decimal.TryParse(value.ToString(), NumberStyles.Currency, CultureInfo.CurrentCulture, out _))
				fieldType = typeof(decimal);
			else if (DateTime.TryParse(value.ToString(), out _))
				fieldType = typeof(DateTime);
			else {
				if (value.ToString().Length == 1)
					fieldType = typeof(char);
				else
					fieldType = typeof(string);
			}

			if (fieldType == typeof(string))
				return fieldType;
			else if (fieldType == typeof(decimal)) {
				if (existingType == null)
					return fieldType;
				else if (existingType != typeof(decimal))
					return typeof(string);
			} else if (fieldType == typeof(Guid)) {
				if (existingType == null)
					return fieldType;
				else if (existingType != typeof(Guid))
					return typeof(string);
			} else if (fieldType == typeof(DateTime)) {
				if (existingType == null)
					return fieldType;
				else if (existingType != typeof(DateTime))
					return typeof(string);
			} else if (fieldType == typeof(decimal)) {
				if (existingType == null)
					return fieldType;
				else if (existingType == typeof(DateTime))
					return typeof(string);
				else if (existingType == typeof(double)
					|| existingType == typeof(float)
					|| existingType == typeof(long)
					|| existingType == typeof(bool))
					return fieldType;
			} else if (fieldType == typeof(double)) {
				if (existingType == null)
					return fieldType;
				else if (existingType == typeof(DateTime))
					return typeof(string);
				else if (existingType == typeof(long)
					|| existingType == typeof(bool))
					return fieldType;
			} else if (fieldType == typeof(float)) {
				if (existingType == null)
					return fieldType;
				else if (existingType == typeof(DateTime))
					return typeof(string);
				else if (existingType == typeof(long)
					|| existingType == typeof(bool))
					return fieldType;
			} else if (fieldType == typeof(long)) {
				if (existingType == null)
					return fieldType;
				else if (existingType == typeof(DateTime))
					return typeof(string);
				else if (existingType == typeof(bool))
					return fieldType;
			} else if (fieldType == typeof(bool)) {
				if (existingType == null)
					return fieldType;
			} else if (fieldType == typeof(char)) {
				if (existingType == null)
					return fieldType;
			} else return typeof(string);

			if (isLastScanRow && existingType == null) {
				return typeof(string);
			}
			return fieldType;
		}

		internal virtual bool Produce(object[] buffer)
		{
			return DoProduce(buffer);
		}

		protected bool DoProduce(object[] buffer)
		{
			string line;
			if (_index < _analyzeBuffer.Length) {
				line = _analyzeBuffer[_index];
				++_index;
			} else if (!_lineReader.MoveNext()) {
				return false;
			} else { 
				line = _lineReader.Current;
			}
#if NET8_0_OR_GREATER
			if (!_config.UsesQuotes) {
				FastProduceValues(line, buffer);
				return true;
			}
#endif
			var values = GetFieldValues(line);
			for (int i = 0; i < values.Length; ++i) {
				if (i >= buffer.Length || i >= FieldTypes.Length) {
					break;
				}
				_fieldReader[i](values[i], buffer);
			}
			return true;
		}

#if NET8_0_OR_GREATER
		private void FastProduceValues(string line, object[] buffer)
		{
			Span<Range> subStrings = stackalloc Range[_fieldReader.Length];
			ReadOnlySpan<char> lineSpan = line;
			lineSpan.Split(subStrings, _config.Delimiter, _config.Trim ? StringSplitOptions.TrimEntries : StringSplitOptions.None);
			for (int i = 0; i < _fieldReader.Length; ++i) {
				var range = subStrings[i];
				_fieldReader[i](lineSpan.Slice(range.Start.Value, range.End.Value - range.Start.Value), buffer);
			}
		}
#endif

		private IEnumerable<string> ReadLines(string? EOLDelimiter = null, char quoteChar = '\0', bool mayContainEOLInData = false, int maxLineSize = 32768,
			char escapeChar = '\\')
		{
			EOLDelimiter = EOLDelimiter ?? Environment.NewLine;

			if (!mayContainEOLInData) {
				string? line;
				if (EOLDelimiter == Environment.NewLine || (EOLDelimiter == "\r") || (EOLDelimiter == "\n")) {
					while ((line = _reader.ReadLine()) != null)
						yield return line;
				} else {
					while ((line = ReadLineWithFixedNewlineDelimiter(_reader, EOLDelimiter, maxLineSize)) != null)
						yield return line;
				}
			}

			bool isPrevEscape = false;
			bool inQuote = false;
			List<char> buffer = new List<char>();
			char c = '\0';
			CircularBuffer<char> delim_buffer = new CircularBuffer<char>(EOLDelimiter.Length);
			while (_reader.Peek() >= 0) {
				isPrevEscape = c == escapeChar;
				c = (char)_reader.Read();
				delim_buffer.Enqueue(c);
				if (quoteChar != '\0' && quoteChar == c) {
					if (!isPrevEscape)
						inQuote = !inQuote;
					else if (_reader.Peek() == quoteChar)
						inQuote = false;
					else
						inQuote = true;
				}

				if (!inQuote) {
					if (delim_buffer.ToString() == EOLDelimiter) {
						if (buffer.Count > 0) {
							string x = new String(buffer.ToArray());
							yield return x.Substring(0, x.Length - (EOLDelimiter.Length - 1));
							buffer.Clear();
						}
						continue;
					}
				}
				buffer.Add(c);

				if (buffer.Count > maxLineSize)
					throw new ApplicationException("Large line found. Check and correct the end of line delimiter.");
			}

			if (buffer.Count > 0)
				yield return new String(buffer.ToArray());
			else
				yield break;
		}

		private static string? ReadLineWithFixedNewlineDelimiter(TextReader reader, string delim, int maxLineSize = 32768)
		{
			if (reader.Peek() == -1)
				return null;
			if (string.IsNullOrEmpty(delim))
				return reader.ReadToEnd();

			var sb = new StringBuilder();
			var delimCandidatePosition = 0;
			while (reader.Peek() != -1 && delimCandidatePosition < delim.Length) {
				var c = (char)reader.Read();
				if (c == delim[delimCandidatePosition]) {
					delimCandidatePosition++;
				} else {
					delimCandidatePosition = 0;
				}
				sb.Append(c);
				if (sb.Length > maxLineSize)
					throw new ApplicationException("Large line found. Check and correct the end of line delimiter.");
			}
			return sb.ToString(0, sb.Length - (delimCandidatePosition == delim.Length ? delim.Length : 0));
		}

		public void Dispose()
		{
			_reader.Dispose();
			GC.SuppressFinalize(this);
		}

		private class CircularBuffer<T> : Queue<T>
		{
			private int _capacity;

			public CircularBuffer(int capacity)
				: base(capacity)
			{
				_capacity = capacity;
			}

			new public void Enqueue(T item)
			{
				if (_capacity > 0 && base.Count == _capacity)
				{
					base.Dequeue();
				}
				base.Enqueue(item);
			}

			public override string ToString()
			{
				StringBuilder items = new StringBuilder();
				foreach (var x in this)
				{
					items.Append(x);
				};
				return items.ToString();
			}
		}
	}

	internal class PipelinedCsvArrayProducer : CsvArrayProducer
	{
		private ChannelReader<object[]?[]> _reader;
		private ChannelWriter<object[]?[]> _writer;
		private int _blockSize;

		public PipelinedCsvArrayProducer(TextReader reader, CsvConfigurator config) : base(reader, config)
		{
			var options = new BoundedChannelOptions(2)
			  { AllowSynchronousContinuations = true, SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait };
			var channel = Channel.CreateBounded<object[]?[]>(options);
			_reader = channel.Reader;
			_writer = channel.Writer;
			_blockSize = config.Pipelined;
			Task.Run(ProducePipeline);
		}

		private async Task ProducePipeline()
		{
			var pool = ArrayPool<object[]?>.Shared;
			var more = true;
			while (more) {
				var block = pool.Rent(_blockSize);
				for (int i = 0; i < block.Length; ++i) {
					var buffer = block[i];
					if (buffer == null) {
						buffer = new object[FieldCount];
						block[i] = buffer;
					} else if (buffer.Length != FieldCount) {
						Array.Resize(ref buffer, FieldCount);
						block[i] = buffer;
					}
					if (!DoProduce(buffer)) {
						block[i] = null;
						more = false;
						break;
					}
				}
				await _writer.WriteAsync(block);
			}
			_writer.Complete();
		}

		private object[]?[]? _block;
		private int _blockIdx;

		internal override bool Produce(object[] buffer)
		{
			if (_block == null || _blockIdx == _block.Length) {
				if (_reader.Completion.IsCompleted || _reader.Completion.IsFaulted) {
					return false;
				}
				if (_block != null) {
					ArrayPool<object[]?>.Shared.Return(_block);
				}
				if (!_reader.TryRead(out _block)) {
					var more = _reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult();
					if (!more) {
						return false;
					}
					if (!_reader.TryRead(out _block)) {
						throw new Exception("Failed to read from pipeline");
					}
				}
				_blockIdx = 0;
			}
			var oldBuffer = _block[_blockIdx];
			if (oldBuffer == null) {
				return false;
			}
			Array.Copy(oldBuffer, buffer, FieldCount);
			++_blockIdx;
			return true;
		}
	}
}
