using System.ComponentModel;

using Pansynchro.Core;

namespace Pansynchro.Connectors.TextFile.CSV
{
    internal class CsvConfigurator : CustomConfiguratorBase
    {
        public CsvConfigurator() { }

        public CsvConfigurator(string conf) : this()
        {
            ConnectionString = conf;
        }

        [Category("CSV")]
		[Description("The character used to separate CSV fields")]
        [DefaultValue(',')]
		public char Delimiter
        {
            get => GetChar(nameof(Delimiter), ',');
            set => this[nameof(Delimiter)] = value;
        }

        [Category("CSV")]
		[DisplayName("Auto-detect delimiter")]
		[Description("If true, Pansynchro will analyze the file to discover its delimiter rather than using the Delimiter property")]
        [DefaultValue(true)]
		public bool AutoDetectDelimiter
        {
            get => GetBool(nameof(AutoDetectDelimiter));
            set => this[nameof(AutoDetectDelimiter)] = value;
        }

        [Category("CSV")]
        [Description("If true, treat the first line of the file as a header")]
        [DefaultValue(false)]
        public bool UsesHeader
        {
            get => GetBool(nameof(UsesHeader));
            set => this[nameof(UsesHeader)] = value;
        }

        [Category("CSV")]
        [Description("If true, check columns for quoted data")]
        [DefaultValue(true)]
        public bool UsesQuotes
        {
            get => GetBool(nameof(UsesQuotes));
            set => this[nameof(UsesQuotes)] = value;
        }

        [Category("CSV")]
        [DisplayName("Quote character")]
        [Description("The character used to quote an entire field")]
        [DefaultValue('"')]
        public char QuoteChar
        {
            get => GetChar(nameof(QuoteChar), '"');
            set => this[nameof(QuoteChar)] = value;
        }

        [Category("CSV")]
        [DisplayName("Escape character")]
        [Description("The character used to escape quote marks inside a quote")]
        [DefaultValue('\\')]
        public char EscapeChar
        {
            get => GetChar(nameof(EscapeChar), '\\');
            set => this[nameof(EscapeChar)] = value;
        }
    }
}