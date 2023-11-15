using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Pansynchro.PanSQL.Core
{
	internal class NullableDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey?, TValue>>
	{
		private readonly Dictionary<TKey, TValue> _dictionary = new();
		private TValue? _nullValue;
		private bool _nullSet;

		public TValue this[TKey? key] {
			get => key is null ? (_nullSet ? _nullValue : throw new KeyNotFoundException())! : _dictionary[key];
			set {
				if (key is null) {
					_nullSet = true;
					_nullValue = value;
				} else {
					_dictionary[key] = value;
				}
			}
		}

		public void Add(TKey? key, TValue value)
		{
			if (key is null) {
				if (_nullSet) {
					throw new System.ArgumentException("Null key already exists in this dictionary");
				} else {
					_nullSet = true;
					_nullValue = value;
				}
			} else { 
				_dictionary.Add(key, value);
			}
		}

		public IEnumerator<KeyValuePair<TKey?, TValue>> GetEnumerator()
		{
			if (_nullSet) {
				yield return new KeyValuePair<TKey?, TValue>(default, _nullValue!);
			}
			foreach (var pair in _dictionary) {
				yield return new KeyValuePair<TKey?, TValue>(pair.Key, pair.Value);
			}
		}

		public bool TryGetValue(TKey? key, [MaybeNullWhen(false)] out TValue? value)
		{
			if (key is null) {
				if (_nullSet) {
					value = _nullValue;
					return true;
				} else {
					value = default;
					return false;
				}
			}
			return _dictionary.TryGetValue(key, out value);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}
