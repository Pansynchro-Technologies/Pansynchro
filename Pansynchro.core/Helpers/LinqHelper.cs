using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Pansynchro.Core.Helpers
{
    public static class LinqHelper
    {
        /// <summary>
        /// Groups the adjacent elements of a sequence according to a specified
        /// key selector function.
        /// </summary>
        /// <remarks>
        /// The groups don't contain buffered elements.
        /// Enumerating the groups in the correct order is mandatory.
        /// 
        /// Adapted from code found at https://stackoverflow.com/a/72347137/32914
        /// </remarks>
        internal static IAsyncEnumerable<IAsyncGrouping<TKey, TSource>>
            LazyGroupAdjacent<TSource, TKey>(
                this IAsyncEnumerable<TSource> source,
                Func<TSource, TKey> keySelector,
                IEqualityComparer<TKey>? keyComparer = null)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            keyComparer ??= EqualityComparer<TKey>.Default;
            return Implementation();

            async IAsyncEnumerable<IAsyncGrouping<TKey, TSource>> Implementation(
                [EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                Tuple<TSource?, TKey?, bool>? sharedState = null;
                var enumerator = source.GetAsyncEnumerator(cancellationToken);
                try
                {
                    if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                        yield break;
                    var firstItem = enumerator.Current;
                    var firstKey = keySelector(firstItem);
                    sharedState = new(firstItem, firstKey, true);

                    Tuple<TSource?, TKey?, bool>? previousState = null;
                    while (true)
                    {
                        var state = Volatile.Read(ref sharedState);
                        if (ReferenceEquals(state, previousState))
                            throw new InvalidOperationException("Out of order enumeration.");
                        var (item, key, exists) = state;
                        if (!exists) yield break;
                        previousState = state;
                        yield return new AsyncGrouping<TKey, TSource>(key!, GetAdjacent(state!));
                    }
                }
                finally { await enumerator.DisposeAsync().ConfigureAwait(false); }

                async IAsyncEnumerable<TSource> GetAdjacent(Tuple<TSource, TKey, bool> state)
                {
                    if (!ReferenceEquals(Volatile.Read(ref sharedState), state))
                        throw new InvalidOperationException("Out of order enumeration.");
                    var (stateItem, stateKey, stateExists) = state;
                    Debug.Assert(stateExists);
                    yield return stateItem;
                    Tuple<TSource?, TKey?, bool> nextState;
                    while (true)
                    {
                        if (!ReferenceEquals(Volatile.Read(ref sharedState), state))
                            throw new InvalidOperationException("Out of order enumeration.");
                        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                        {
                            nextState = new(default, default, false);
                            break;
                        }
                        var item = enumerator.Current;
                        var key = keySelector(item);
                        if (!keyComparer.Equals(key, stateKey))
                        {
                            nextState = new(item, key, true);
                            break;
                        }
                        yield return item;
                    }
                    if (!ReferenceEquals(Interlocked.CompareExchange(
                        ref sharedState, nextState, state), state))
                        throw new InvalidOperationException("Out of order enumeration.");
                }
            }
        }

        private class AsyncGrouping<TKey, TElement> : IAsyncGrouping<TKey, TElement>
        {
            private readonly TKey _key;
            private readonly IAsyncEnumerable<TElement> _sequence;

            public AsyncGrouping(TKey key, IAsyncEnumerable<TElement> sequence)
            {
                _key = key;
                _sequence = sequence;
            }

            public TKey Key => _key;

            public IAsyncEnumerator<TElement> GetAsyncEnumerator(
                CancellationToken cancellationToken = default)
            {
                return _sequence.GetAsyncEnumerator(cancellationToken);
            }
        }

        public static IEnumerable<int> IndexWhere<T>(this IEnumerable<T> source, Func<T, bool> filter)
        {
            ArgumentNullException.ThrowIfNull(source, nameof(source));
            ArgumentNullException.ThrowIfNull(filter, nameof(filter));
            return IndexWhereImpl(source, filter);
        }

        private static IEnumerable<int> IndexWhereImpl<T>(IEnumerable<T> source, Func<T, bool> filter)
        {
            int i = 0;
            foreach (var item in source) {
                if (filter(item)) {
                    yield return i;
                }
                ++i;
            }
        }
    }
}
