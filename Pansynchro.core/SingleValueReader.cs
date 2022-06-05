using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core
{
    public class SingleValueReader : ArrayReader
    {
        private readonly string _name;

        public SingleValueReader(string name, object value)
        {
            _name = name;
            _buffer = new object[1] { value };
        }

        private bool _readOnce;

        public override int RecordsAffected => 1;

        public override bool Read()
        {
            var result = !_readOnce;
            _readOnce = true;
            return result;
        }

        public override string GetName(int i) => _name;

        public override int GetOrdinal(string name) => name == _name ? 0 : -1;

        public override void Dispose()
        { }
    }
}
