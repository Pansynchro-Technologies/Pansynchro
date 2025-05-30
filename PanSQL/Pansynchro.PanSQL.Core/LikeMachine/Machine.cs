using System;
using System.Collections.Generic;

namespace Pansynchro.PanSQL.Core.LikeMachine;
internal class Machine(List<Step> steps)
{
	private readonly Step[] _steps = [.. steps];

	public bool Matches(string value) => _steps[0].Matches(value, _steps);
}

internal abstract class Step
{
	internal abstract bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps);
}

internal class LiteralMatch(string value) : Step
{
	private readonly string _value = value;

	internal override bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps)
	{
		if (!input.StartsWith(_value, StringComparison.InvariantCulture)) {
			return false;
		}
		if (steps.Length == 1) {
			return input.Length == _value.Length;
		}
		input = input[_value.Length..];
		steps = steps[1..];
		return steps[0].Matches(input, steps);
	}
}

internal class IndefiniteMatch() : Step
{
	internal override bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps)
	{
		var spaces = 0;
		int? next = null;
		for (int i = 1; i < steps.Length; ++i) {
			if (steps[i] is SingleMatch) {
				++spaces;
			} else if (steps[i] is IndefiniteMatch) {
			} else {
				next = i;
				break;
			}
		}
		if (input.Length < spaces) {
			return false;
		}
		if (next == null) { //matching the rest of the string with this wildcard, with no literals after it
			return true;
		}
		steps = steps[next.Value..];
		var nextStep = steps[0];
		if (spaces > 0) {
			input = input[spaces..];
		}
		while (input.Length > 0) {
			if (nextStep.Matches(input, steps)) {
				return true;
			}
			input = input[1..];
		}
		return false;
	}
}

internal class SingleMatch() : Step
{
	internal override bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps)
	{
		if (input.Length == 0) {
			return false;
		}
		if (steps.Length == 1) {
			return input.Length == 1;
		}
		input = input[1..];
		steps = steps[1..];
		return steps[0].Matches(input, steps);
	}
}

internal class SetStep(HashSet<char> values) : Step
{
	public HashSet<char> Values => values;

	internal override bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps)
	{
		if (input.Length == 0 || !Values.Contains(input[0])) {
			return false;
		}
		if (steps.Length == 1) {
			return input.Length == 1;
		}
		input = input[1..];
		steps = steps[1..];
		return steps[0].Matches(input, steps);
	}
}

internal class NotSetStep(HashSet<char> values) : Step
{
	public HashSet<char> Values => values;

	internal override bool Matches(ReadOnlySpan<char> input, ReadOnlySpan<Step> steps)
	{
		if (input.Length == 0 || Values.Contains(input[0])) {
			return false;
		}
		if (steps.Length == 1) {
			return input.Length == 1;
		}
		input = input[1..];
		steps = steps[1..];
		return steps[0].Matches(input, steps);
	}
}
