using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
    public class MssqlFormatter : ISqlFormatter
    {
        public static MssqlFormatter Instance { get; } = new();
 
        private MssqlFormatter() { }

        public string QuoteName(string name)
        {
            return '[' + name + ']';
        }
    }
}
