namespace Pansynchro.Core
{
	public record StreamDescription(string? Namespace, string Name)
	{
		public static StreamDescription Parse(string name)
		{
			var pieces = name.Split('.');
			if (pieces.Length == 1) {
				return new StreamDescription(null, pieces[0]);
			}
			if (pieces.Length == 2) {
				return new StreamDescription(pieces[0], pieces[1]);
			}
			return new StreamDescription(string.Join('.', pieces[0..^2]), pieces[pieces.Length - 1]);
		}

		public override string ToString()
		{
			return string.IsNullOrEmpty(Namespace) ? Name : Namespace + "." + Name;
		}
	}
}
