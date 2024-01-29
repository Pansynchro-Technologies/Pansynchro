using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Pansynchro.PanSQL.Core
{
	public static class Aggregates
	{
		public interface IAggregate<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
		{
			TValue Get(TKey key);
		}

		public static MaxAggregate<TKey, TValue> Max<TKey, TValue>()
			where TKey : notnull where TValue : IComparable<TValue>
			=> new();

		public readonly struct MaxAggregate<TKey, TValue> : IAggregate<TKey, TValue> where TKey : notnull where TValue : IComparable<TValue>
		{
			private readonly Dictionary<TKey, TValue> _dictionary = new();

			public MaxAggregate() { }

			public void Add(TKey key, TValue value)
			{
				ref var old = ref CollectionsMarshal.GetValueRefOrAddDefault(_dictionary, key, out var found);
				if (found) {
					if (old is null || old.CompareTo(value) < 0) {
						old = value;
					}
				} else {
					old = value;
				}
			}

			public TValue Get(TKey key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => _dictionary.GetEnumerator();
		}

		public static MaxAggregateNullable<TKey, TValue> MaxNullable<TKey, TValue>()
			where TValue : IComparable<TValue>
			=> new();

		public readonly struct MaxAggregateNullable<TKey, TValue> : IAggregate<TKey?, TValue> where TValue : IComparable<TValue>
		{
			private readonly NullableDictionary<TKey, TValue?> _dictionary = new();

			public MaxAggregateNullable() { }

			public void Add(TKey? key, TValue value)
			{
				ref var old = ref _dictionary.GetValueRefOrAddDefault(key, out var found);
				if (found) {
					if (old is null || old.CompareTo(value) < 0) {
						old = value;
					}
				} else {
					old = value;
				}
			}

			public TValue? Get(TKey? key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey?, TValue?>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static MinAggregate<TKey, TValue> Min<TKey, TValue>()
			where TKey : notnull where TValue : IComparable<TValue>
			=> new();

		public readonly struct MinAggregate<TKey, TValue> : IAggregate<TKey, TValue> where TKey : notnull where TValue : IComparable<TValue>
		{
			private readonly Dictionary<TKey, TValue> _dictionary = new();

			public MinAggregate() { }

			public void Add(TKey key, TValue value)
			{
				ref var old = ref CollectionsMarshal.GetValueRefOrAddDefault(_dictionary, key, out var found);
				if (found) {
					if (old is null || old.CompareTo(value) > 0) {
						old = value;
					}
				} else {
					old = value;
				}
			}

			public TValue Get(TKey key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => _dictionary.GetEnumerator();
		}

		public static MinAggregateNullable<TKey, TValue> MinNullable<TKey, TValue>()
			where TValue : IComparable<TValue>
			=> new();

		public readonly struct MinAggregateNullable<TKey, TValue> : IAggregate<TKey?, TValue> where TValue : IComparable<TValue>
		{
			private readonly NullableDictionary<TKey, TValue?> _dictionary = new();

			public MinAggregateNullable() { }

			public void Add(TKey? key, TValue value)
			{
				ref var old = ref _dictionary.GetValueRefOrAddDefault(key, out var found);
				if (found) {
					if (old is null || old.CompareTo(value) > 0) {
						old = value;
					}
				} else {
					old = value;
				}
			}

			public TValue? Get(TKey? key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey?, TValue?>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static CountAggregate<TKey> Count<TKey>()
			where TKey : notnull
		=> new();

		public readonly struct CountAggregate<TKey> : IAggregate<TKey, int> where TKey : notnull
		{
			private readonly Dictionary<TKey, int> _dictionary = new();

			public CountAggregate() { }

			public void Add(TKey key)
			{
				ref var old = ref CollectionsMarshal.GetValueRefOrAddDefault(_dictionary, key, out _);
				++old;
			}

			public int Get(TKey key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey, int>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static CountAggregateNullable<TKey> CountNullable<TKey>()
			=> new();

		public readonly struct CountAggregateNullable<TKey> : IAggregate<TKey?, int>
		{
			private readonly NullableDictionary<TKey, int> _dictionary = new();

			public CountAggregateNullable() { }

			public void Add(TKey key)
			{
				ref var old = ref _dictionary.GetValueRefOrAddDefault(key, out _);
				++old;
			}

			public int Get(TKey? key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey?, int>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static SumAggregate<TKey, TValue> Sum<TKey, TValue>()
			where TKey : notnull where TValue : INumber<TValue>
			=> new();

		public readonly struct SumAggregate<TKey, TValue> : IAggregate<TKey, TValue> where TKey : notnull where TValue : INumber<TValue>
		{
			private readonly Dictionary<TKey, TValue> _dictionary = new();

			public SumAggregate() { }

			public void Add(TKey key, TValue value)
			{
				ref var old = ref CollectionsMarshal.GetValueRefOrAddDefault(_dictionary, key, out _);
				old = old is null ? value : old + value;
			}

			public TValue Get(TKey key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => _dictionary.GetEnumerator();
		}

		public static SumAggregateNullable<TKey, TValue> SumNullable<TKey, TValue>()
			where TKey : notnull where TValue : INumber<TValue>
			=> new();

		public readonly struct SumAggregateNullable<TKey, TValue> : IAggregate<TKey?, TValue> where TValue : INumber<TValue>
		{
			private readonly NullableDictionary<TKey, TValue> _dictionary = new();

			public SumAggregateNullable() { }

			public void Add(TKey key, TValue value)
			{
				ref var old = ref _dictionary.GetValueRefOrAddDefault(key, out _);
				old = old is null ? value : old + value;
			}

			public TValue Get(TKey? key) => _dictionary[key];

			public IEnumerator<KeyValuePair<TKey?, TValue>> GetEnumerator() => _dictionary.GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => _dictionary.GetEnumerator();
		}

		public static AverageAggregate<TKey, TValue> Avg<TKey, TValue>()
			where TKey : notnull where TValue : INumber<TValue>
			=> new();

		public readonly struct AverageAggregate<TKey, TValue> : IAggregate<TKey, TValue> where TKey : notnull where TValue : INumber<TValue>
		{
			private readonly SumAggregate<TKey, TValue> _sum = new();
			private readonly CountAggregate<TKey> _count = new();

			public AverageAggregate() { }

			public void Add(TKey key, TValue value)
			{
				_sum.Add(key, value);
				_count.Add(key);
			}

			public TValue Get(TKey key) => _sum.Get(key) / TValue.CreateChecked(_count.Get(key));

			public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
			{
				foreach (var pair in _sum) {
					yield return KeyValuePair.Create(pair.Key, pair.Value / TValue.CreateChecked(_count.Get(pair.Key)));
				}
			}

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static AverageAggregateNullable<TKey, TValue> AvgNullable<TKey, TValue>()
			where TValue : INumber<TValue>
			=> new();

		public readonly struct AverageAggregateNullable<TKey, TValue> : IAggregate<TKey?, TValue> where TValue : INumber<TValue>
		{
			private readonly SumAggregateNullable<TKey, TValue> _sum = new();
			private readonly CountAggregateNullable<TKey> _count = new();

			public AverageAggregateNullable() { }

			public void Add(TKey key, TValue value)
			{
				_sum.Add(key, value);
				_count.Add(key);
			}

			public TValue Get(TKey? key) => _sum.Get(key) / TValue.CreateChecked(_count.Get(key));

			public IEnumerator<KeyValuePair<TKey?, TValue>> GetEnumerator()
			{
				foreach (var pair in _sum) {
					yield return KeyValuePair.Create(pair.Key, pair.Value / TValue.CreateChecked(_count.Get(pair.Key)));
				}
			}

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static String_aggAggregate<TKey, TValue> String_agg<TKey, TValue>(string separator)
			where TKey : notnull
			=> new(separator);

		public readonly struct String_aggAggregate<TKey, TValue> : IAggregate<TKey, string> where TKey: notnull
		{
			private readonly Dictionary<TKey, List<TValue>> _dictionary = [];
			private readonly string _separator;

			public String_aggAggregate(string separator)
			{
				_separator = separator;
			}

			public void Add(TKey key, TValue value)
			{
				if (!_dictionary.TryGetValue(key, out var list)) {
					list = new();
					_dictionary.Add(key, list);
				}
				list.Add(value);
			}

			public string Get(TKey key) => string.Join(_separator, _dictionary[key]);

			public IEnumerator<KeyValuePair<TKey, string>> GetEnumerator()
			{
				foreach (var pair in _dictionary) {
					yield return KeyValuePair.Create(pair.Key, string.Join(_separator, pair.Value));
				}
			}

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2>>>
			Combine<TKey, A1, A2>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key)));
			}
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2, A3>>>
			Combine<TKey, A1, A2, A3>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2, IAggregate<TKey, A3> agg3)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key), agg3.Get(pair.Key)));
			}
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2, A3, A4>>>
			Combine<TKey, A1, A2, A3, A4>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2, IAggregate<TKey, A3> agg3, IAggregate<TKey, A4> agg4)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key), agg3.Get(pair.Key), agg4.Get(pair.Key)));
			}
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2, A3, A4, A5>>>
			Combine<TKey, A1, A2, A3, A4, A5>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2, IAggregate<TKey, A3> agg3, IAggregate<TKey, A4> agg4, IAggregate<TKey, A5> agg5)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key), agg3.Get(pair.Key), agg4.Get(pair.Key), agg5.Get(pair.Key)));
			}
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2, A3, A4, A5, A6>>>
			Combine<TKey, A1, A2, A3, A4, A5, A6>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2, IAggregate<TKey, A3> agg3, IAggregate<TKey, A4> agg4, IAggregate<TKey, A5> agg5, IAggregate<TKey, A6> agg6)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key), agg3.Get(pair.Key), agg4.Get(pair.Key), agg5.Get(pair.Key), agg6.Get(pair.Key)));
			}
		}

		public static IEnumerable<KeyValuePair<TKey, ValueTuple<A1, A2, A3, A4, A5, A6, A7>>>
			Combine<TKey, A1, A2, A3, A4, A5, A6, A7>(IAggregate<TKey, A1> agg1, IAggregate<TKey, A2> agg2, IAggregate<TKey, A3> agg3, IAggregate<TKey, A4> agg4, IAggregate<TKey, A5> agg5, IAggregate<TKey, A6> agg6, IAggregate<TKey, A7> agg7)
		{
			foreach (var pair in agg1) {
				yield return KeyValuePair.Create(pair.Key, (pair.Value, agg2.Get(pair.Key), agg3.Get(pair.Key), agg4.Get(pair.Key), agg5.Get(pair.Key), agg6.Get(pair.Key), agg7.Get(pair.Key)));
			}
		}
	}
}
