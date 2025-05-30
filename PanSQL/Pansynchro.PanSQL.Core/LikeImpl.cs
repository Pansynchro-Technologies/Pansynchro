using System.Collections.Generic;

using Pansynchro.PanSQL.Core.LikeMachine;

namespace Pansynchro.PanSQL.Core;
public static class LikeImpl
{
	private static readonly Dictionary<(string, char), Machine?> _cache = [];

	public static bool Like(string? s, string? match, char escape = '\\')
	{
		if (s == null || match == null) {
			return false;
		}
		var key = (match, escape);
		if (!_cache.TryGetValue(key, out var machine)) {
			machine = LikeMachineParser.Parse(match, escape);
			_cache.Add(key, machine);
		}
		return machine != null && machine.Matches(s);
	}
}
