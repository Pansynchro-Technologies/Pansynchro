using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core
{
	public interface ITransformer
	{
		IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input);
	}

	public abstract class StreamTransformerBase : ITransformer
	{
		protected Dictionary<string, Func<IDataReader, IEnumerable<object[]>>> _streamDict
			= new(System.StringComparer.InvariantCultureIgnoreCase);
		protected Dictionary<StreamDescription, StreamDescription> _nameMap = new();
		private Dictionary<string, string?> _nsMap = new();
		private string? _nullNsMap;
		protected readonly DataDictionary _destDict;

		public StreamTransformerBase(DataDictionary destDict)
		{
			_destDict = destDict;
		}

		public virtual async IAsyncEnumerable<DataStream> StreamFirst()
		{
			await Task.CompletedTask; //just to shut the compiler up
			yield break;
		}

		public virtual async IAsyncEnumerable<DataStream> StreamLast()
		{
			await Task.CompletedTask; //just to shut the compiler up
			yield break;
		}

		public async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
		{
			await foreach (var stream in StreamFirst()) {
				yield return stream;
			}
			await foreach (var stream in input) {
				DataStream result;
				Console.WriteLine($"Processing Stream: {stream.Name}");
				if (!_nameMap.TryGetValue(stream.Name, out var destName)) {
					if (stream.Name.Namespace == null) {
						if (_nullNsMap != null) {
							destName = stream.Name with { Namespace = _nullNsMap };
						}
					} else if (_nsMap.TryGetValue(stream.Name.Namespace, out var newNs)) {
						destName = stream.Name with { Namespace = newNs };
					}
				}
				if (_streamDict.TryGetValue(stream.Name.ToString(), out var processor)) {
					var destStream = _destDict.GetStream(destName ?? stream.Name, NameStrategy.Get(NameStrategyType.Identity));
					if (destStream == null) {
						Console.WriteLine($"Stream name '{destName}' not found in data dictionary");
						result = stream;
					} else {
						Console.WriteLine("Stream found");
						result = stream.Transformed(processor, destStream);
					}
				} else {
					Console.WriteLine("Stream not found");
					result = stream;
				}
				yield return destName != null ? result with { Name = destName } : result;
			}
			await foreach (var stream in StreamLast())	{
				yield return stream;
			}
		}

		protected void MapNamespaces(string? l, string? r) 
		{
			if (l == null) {
				if (_nullNsMap != null) {
					throw new Exception($"Namespace 'null' has already been mapped to {_nullNsMap}.");
				}
				_nullNsMap = r;
			} else {
				_nsMap[l] = r;
			}
		}
	}
}
