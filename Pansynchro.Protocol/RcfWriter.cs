using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Pansynchro.Protocol
{
	public interface IRcfWriter
	{
		int NewData { get; }
		void FinishBlock(BinaryWriter bw);
	}

	public class RcfWriter<T> : IRcfWriter
	{
#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.
		private Dictionary<T, int> _cache = new();
#pragma warning restore CS8714

		private int _lastSize = 0;
		private Action<object, BinaryWriter> _writer;

		public RcfWriter(Action<object, BinaryWriter> writer)
		{
			_writer = writer;
		}

		public void Write(object o, BinaryWriter writer)
		{
			var value = (T)o;
			if (!_cache.TryGetValue(value, out var idx)) {
				idx = _cache.Count;
				_cache.Add(value, idx);
			}
			writer.Write7BitEncodedInt(idx);
		}

		int IRcfWriter.NewData => _cache.Count - _lastSize;

		public void FinishBlock(BinaryWriter bw)
		{
			foreach (var pair in _cache.Where(p => p.Value >= _lastSize).OrderBy(p => p.Value)) {
				_writer(pair.Key!, bw);
			}
			_lastSize = _cache.Count;
		}
	}

	public class NullableRcfWriter<T> : IRcfWriter
	{
#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.
		private Dictionary<T, int> _cache = new();
#pragma warning restore CS8714

		private int _lastSize = 0;
		private Action<object?, BinaryWriter> _writer;

		public NullableRcfWriter(Action<object?, BinaryWriter> writer)
		{
			_writer = writer;
		}

		public void Write(object? o, BinaryWriter writer)
		{
			if (o is null || o == System.DBNull.Value) {
				writer.Write((byte)0);
			} else {
				var value = (T)o;
				if (!_cache.TryGetValue(value, out var idx)) {
					idx = _cache.Count + 1;
					_cache.Add(value, idx);
				}
				writer.Write7BitEncodedInt(idx);
			}
		}

		int IRcfWriter.NewData => _cache.Count - _lastSize;

		public void FinishBlock(BinaryWriter bw)
		{
			foreach (var pair in _cache.Where(p => p.Value > _lastSize).OrderBy(p => p.Value)) {
				_writer(pair.Key!, bw);
			}
			_lastSize = _cache.Count;
		}
	}
}
