using System.Globalization;

namespace Pansynchro.Core
{
    public record StreamDescription(string? Namespace, string Name)
    {
        public StreamDescription ToLower() => new(Namespace?.ToLower(CultureInfo.InvariantCulture), Name.ToLower(CultureInfo.InvariantCulture));

        public static StreamDescription Parse(string name)
        {
            var pieces = name.Split('.');
            return pieces.Length == 1 ? new StreamDescription(null, pieces[0]) : new StreamDescription(pieces[0], pieces[1]);
        }

        public override string ToString()
        {
            return Namespace == null ? Name : Namespace + "." + Name;
        }
    }
}
