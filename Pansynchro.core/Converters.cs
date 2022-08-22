using System;
using System.Runtime.InteropServices;

namespace Pansynchro.Core
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public readonly struct DecimalConverter
    {
        [FieldOffset(0)]
        private readonly decimal _value;
        [FieldOffset(0)]
        private readonly ulong _low;
        [FieldOffset(8)]
        private readonly ulong _high;

        public decimal Value => _value;
        public ulong Low => _low;
        public ulong High => _high;

        public DecimalConverter(decimal value)
        {
            _low = 0;
            _high = 0;
            _value = value;
        }

        public DecimalConverter(ulong low, ulong high)
        {
            _value = 0;
            _low = low;
            _high = high;
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public readonly struct GuidConverter
    {
        [FieldOffset(0)]
        private readonly Guid _value;
        [FieldOffset(0)]
        private readonly ulong _low;
        [FieldOffset(8)]
        private readonly ulong _high;

        public Guid Value => _value;
        public ulong Low => _low;
        public ulong High => _high;

        public GuidConverter(Guid value)
        {
            _low = 0;
            _high = 0;
            _value = value;
        }

        public GuidConverter(ulong low, ulong high)
        {
            _value = Guid.Empty;
            _low = low;
            _high = high;
        }
    }
}
