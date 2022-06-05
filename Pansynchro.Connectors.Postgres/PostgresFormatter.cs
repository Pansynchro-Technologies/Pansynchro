using System;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Postgres
{
    public class PostgresFormatter : ISqlFormatter
    {
        public static PostgresFormatter Instance { get; } = new();

        private PostgresFormatter() { }

        public string QuoteName(string name)
        {
            return '"' + name + '"';
        }
    }
}
