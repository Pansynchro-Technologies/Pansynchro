using Pansynchro.SQL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Connectors.Sqlite
{
    public class SqliteFormatter : ISqlFormatter
    {
        public static SqliteFormatter Instance { get; } = new();

        private SqliteFormatter() { }

        public string QuoteName(string name)
        {
            return '"' + name + '"';
        }
    }
}
