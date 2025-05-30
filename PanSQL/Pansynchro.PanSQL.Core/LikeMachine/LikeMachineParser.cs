using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.PanSQL.Core.LikeMachine;
internal static class LikeMachineParser
{
	private record struct Token(char Value, bool Literal);

	private static List<Token>? Lex(string matchString, char escape)
	{
		var result = new List<Token>();
		for (int i = 0; i < matchString.Length; ++i) {
			var ch = matchString[i];
			if (ch == escape) {
				++i;
				if (i == matchString.Length) {
					return null;
				}
				result.Add(new(matchString[i], true));
			} else {
				result.Add(new(ch, ch is not ('%' or '_' or '[' or ']' or '-')));
			}
		}
		return result;
	}

	public static Machine? Parse(string matchString, char escape)
	{
		var tokens = Lex(matchString, escape);
		if (tokens == null || tokens.Count == 0) {
			return null;
		}
		var steps = new List<Step>();
		string? literal = null;
		for (int i = 0; i < tokens.Count; ++i) {
			var tok = tokens[i];
			if (tok.Literal || tok.Value is '-' or ']') {
				literal = literal == null ? new string(tok.Value, 1) : literal + tok.Value;
			} else {
				if (literal != null) {
					steps.Add(new LiteralMatch(literal));
					literal = null;
				}
				switch (tok.Value) {
					case '%':
						steps.Add(new IndefiniteMatch());
						break;
					case '_':
						steps.Add(new SingleMatch());
						break;
					case '[':
						var rangeStep = ParseRangeStep(tokens, ref i);
						if (rangeStep == null) {
							return null;
						}
						steps.Add(rangeStep);
						break;
				}
			}
		}
		if (literal != null) {
			steps.Add(new LiteralMatch(literal));
		}
		return new Machine(steps);
	}

	private static Step? ParseRangeStep(List<Token> tokens, ref int i)
	{
		++i;
		var set = new HashSet<char>();
		var negated = false;
		if (i < tokens.Count && tokens[i].Value == '^') {
			++i;
			negated = true;
		}
		while (i < tokens.Count) {
			var ch = tokens[i].Value;
			if (ch == ']') {
				if (set.Count == 0) {
					return null;
				}
				return negated ? new NotSetStep(set) : new SetStep(set);
			}
			if (i + 1 < tokens.Count && tokens[i + 1] is { Literal: false, Value: '-' }) {
				if (i + 2 >= tokens.Count || tokens[i + 2] is { Literal: false, Value: ']' }) {
					return null;
				}
				var end = tokens[i + 2];
				for (var c = ch; c <= end.Value; ++c) {
					set.Add(c);
				}
				i += 2;
			} else {
				set.Add(ch);
			}
			++i;
		}
		return null;
	}
}
