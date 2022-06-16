using Pansynchro.SQL;

namespace Pansynchro.Connectors.Oracle
{
    public class OracleFormatter : ISqlFormatter
    {
        public static OracleFormatter Instance { get; } = new();

        private OracleFormatter() { }

        public string QuoteName(string name)
        {
            return '"' + name + '"';
        }
    }
}
