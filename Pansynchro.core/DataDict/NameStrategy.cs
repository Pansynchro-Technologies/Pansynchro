using System;

namespace Pansynchro.Core.DataDict
{
	public enum NameStrategyType
	{
		Identity,
		LowerCase,
		NameOnly,
		NameOnlyLowerCase
	}

	public abstract class NameStrategy
	{
		protected NameStrategy() { }

		public virtual string MatchingName(StreamDescription name) => MatchingName(name.ToString());
		public abstract string MatchingName(string name);

		private static NameStrategy _identity = new IdentityNameStrategy();
		private static NameStrategy _lowerCase = new LowerCaseNameStrategy();
		private static NameStrategy _nameOnly = new NameOnlyNameStrategy();
		private static NameStrategy _nameOnlyLowerCase = new NameOnlyLowerCaseNameStrategy();

		public static NameStrategy Get(NameStrategyType type) => type switch {
			NameStrategyType.Identity => _identity,
			NameStrategyType.LowerCase => _lowerCase,
			NameStrategyType.NameOnly => _nameOnly,
			NameStrategyType.NameOnlyLowerCase => _nameOnlyLowerCase,
			_ => throw new ArgumentException($"Unknown name strategy type: {type}")
		};

		public static NameStrategy Combine(NameStrategyType t1, NameStrategyType t2)
		{
			if (t1 == t2 || t2 == NameStrategyType.Identity) {
				return Get(t1);
			}
			if (t1 == NameStrategyType.Identity) {
				return Get(t2);
			}
			if ((t1 == NameStrategyType.NameOnly && t2 == NameStrategyType.LowerCase)
				|| (t1 == NameStrategyType.LowerCase && t2 == NameStrategyType.NameOnly)
				|| (t1 == NameStrategyType.NameOnlyLowerCase && t2 is NameStrategyType.NameOnly or NameStrategyType.LowerCase)
				|| (t2 == NameStrategyType.NameOnlyLowerCase && t1 is NameStrategyType.NameOnly or NameStrategyType.LowerCase)) {
				return Get(NameStrategyType.NameOnlyLowerCase);
			}
			throw new ArgumentException($"Unable to combine name strategy types {t1} and {t2}.");
		}
	}

	sealed class IdentityNameStrategy : NameStrategy
	{
		internal IdentityNameStrategy() { }

		public override string MatchingName(string name) => name;
	}

	sealed class LowerCaseNameStrategy : NameStrategy
	{
		internal LowerCaseNameStrategy() { }

		public override string MatchingName(string name) => name.ToLowerInvariant();
	}

	sealed class NameOnlyNameStrategy : NameStrategy
	{
		internal NameOnlyNameStrategy() { }

		public override string MatchingName(string name) => name;

		public override string MatchingName(StreamDescription name) => name.Name.ToString();
	}

	sealed class NameOnlyLowerCaseNameStrategy : NameStrategy
	{
		internal NameOnlyLowerCaseNameStrategy() { }

		public override string MatchingName(string name) => name.ToLowerInvariant();

		public override string MatchingName(StreamDescription name) => name.Name.ToString().ToLowerInvariant();
	}
}
