using System;
using System.Collections.Generic;
using System.Linq;

using DotNet.Globbing;
using DotNet.Globbing.Token;

namespace Pansynchro.Core.Helpers
{
    public class GlobHelper
    {
        public static string ExtractLiteralPrefix(Glob value)
            => PatternToString(ExtractLiteralPrefixTokens(value));

        private static IEnumerable<IGlobToken> ExtractLiteralPrefixTokens(Glob value)
            => value.Tokens.TakeWhile(t => t is LiteralToken or PathSeparatorToken);

        public static string PatternToString(IEnumerable<IGlobToken> pattern)
            => string.Concat(pattern.Select(ExtractStringValue));

        private static string ExtractStringValue(IGlobToken tok)
        {
            if (tok is LiteralToken lt) {
                return lt.Value;
            }
            if (tok is PathSeparatorToken ps) {
                return new string(ps.Value, 1);
            }
            throw new Exception("Should not reach here");
        }

        public static int LastDirSeparatorIndex(Glob pattern)
        {
            var tokens = pattern.Tokens;
            for (int i = tokens.Length - 1; i >= 0; --i) {
                if (tokens[i] is PathSeparatorToken or WildcardDirectoryToken) {
                    return i;
                }
            }
            return -1;
        }

        public static IGlobToken[] ExtractBaseDirectory(Glob pattern)
        {
            var list = ExtractLiteralPrefixTokens(pattern).ToList();
            if (list.Count > 0 && list[list.Count - 1] is not LiteralToken) {
                list.RemoveAt(list.Count - 1);
                if (list.Count > 0 && list[list.Count - 1] is not LiteralToken) {
                    throw new ArgumentException($"Invalid file pattern: {pattern}");
                }
            }
            return list.ToArray();
        }

        public static Glob GetNextPattern(Glob pattern, int patternIndex)
        {
            var list = pattern.Tokens
                .Skip(patternIndex)
                .TakeWhile(t => t is not PathSeparatorToken)
                .ToList();
            var next = patternIndex + list.Count;
            if (next < pattern.Tokens.Length && pattern.Tokens[next] is PathSeparatorToken sep) {
                list.Add(sep);
            }
            return new Glob(list.ToArray());
        }

        public static bool IsLiteralPattern(Glob pattern)
            => pattern.Tokens.Length == 1 && (pattern.Tokens[0] is LiteralToken or PathSeparatorToken);

        public static bool StartsWithLiteralPattern(Glob pattern)
            => pattern.Tokens.Length >= 1 && pattern.Tokens[0] is LiteralToken or PathSeparatorToken;

    }
}
