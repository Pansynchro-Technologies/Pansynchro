using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Pansynchro.Connectors.TextFile.HTML;
internal class HtmlConfigurator : DbConnectionStringBuilder, INotifyPropertyChanged
{
	public ObservableCollection<HtmlConf> Streams { get; set; } = new();

	public HtmlConfigurator()
	{
		Streams.CollectionChanged += this.DictChanged;
		DictChanged(null, null);
	}

	public HtmlConfigurator(string conf) : this()
	{
		ConnectionString = conf;
	}

	private bool _changing = false;

	private void DictChanged(object? sender, NotifyCollectionChangedEventArgs? e)
	{
		if (!_changing) {
			_changing = true;
			try {
				ConnectionString = string.Join(';', Streams.Select(s => $"{s.Name}={JsonSerializer.Serialize(s)}"));
			} finally {
				_changing = false;
			}
		}
	}

	public event PropertyChangedEventHandler? PropertyChanged
	{
		add => ((INotifyPropertyChanged)Streams).PropertyChanged += value;
		remove => ((INotifyPropertyChanged)Streams).PropertyChanged -= value;
	}

	private static readonly JsonSerializerOptions OPTIONS = new() {
		Converters = { new JsonStringEnumConverter() }
	};

	[AllowNull]
	public override object this[string keyword]
	{
		get => keyword == "Streams" ? Streams : base[keyword];
		set {
			if (keyword == "Streams") {
				Streams = (ObservableCollection<HtmlConf>)value!;
			} else {
				var existing = Streams.FirstOrDefault(s => s.Name.Equals(keyword, StringComparison.InvariantCultureIgnoreCase));
				if (existing != null) {
					Streams.Remove(existing);
				}
				var newStream = JsonSerializer.Deserialize<HtmlConf>((string)value!, OPTIONS);
				if (!keyword.Equals(newStream?.Name, StringComparison.InvariantCultureIgnoreCase)) {
					throw new ArgumentException($"Invalid stream name: {keyword}");
				}
				Streams.Add(newStream);
			}
		}
	}

	public override bool TryGetValue(string keyword, [NotNullWhen(true)] out object? value)
	{
		if (keyword == "Streams") {
			value = Streams;
			return true;
		}
		return base.TryGetValue(keyword, out value);
	}

	public class HtmlConf : INotifyPropertyChanged
	{
		public event PropertyChangedEventHandler? PropertyChanged;

		private string _name = "New Stream";
		[Category("HTML")]
		[Description("Name of the stream.  Each stream should have its own unique name.")]
		public string Name
		{
			get => _name;
			set {
				_name = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
			}
		}

		[Category("JSON")]
		[Description("A list of XPath expressions that contain relevant data streams")]
		public ObservableCollection<HtmlQuery> Streams { get; set; } = new();
	}

	public class HtmlQuery : INotifyPropertyChanged
	{
		public event PropertyChangedEventHandler? PropertyChanged;

		public HtmlQuery() { }

		private string _name = "New Path";
		public string Name
		{
			get => _name;
			set {
				_name = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
			}
		}

		private string? _path;
		[Description("XPath expression to extract the relevant data")]
		public string? Path
		{
			get => _path;
			set {
				_path = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Path)));
			}
		}

		private bool _required;
		[Description("If true, the sync will fail with an error if the Path query fails to select any data.")]
		public bool Required
		{
			get => _required;
			set {
				_required = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Required)));
			}
		}

		private DataType _dataType;
		[Description("How to interpret the data retrieved by the query.  If \"Expressions\" is selected, the Expressions values will be applied to the results as sub-queries.")]
		public DataType Type
		{
			get => _dataType;
			set {
				_dataType = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(DataType)));
			}
		}

		public override string ToString() => Name;

		public ObservableCollection<ExpressionQuery>? Expressions { get; set; }
	}

	public class ExpressionQuery: INotifyPropertyChanged
	{
		public event PropertyChangedEventHandler? PropertyChanged;

		private string _name = null!;
		[Description("The name of the field that this expression resolves to.")]
		public string Name
		{
			get => _name;
			set {
				_name = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
			}
		}

		private string _path = null!;
		[Description("XPath expression to extract the relevant values from the data")]
		public string Path
		{
			get => _path;
			set
			{
				_path = value;
				PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Path)));
			}
		}
	}

	public enum DataType
	{
		OuterHtml,
		InnerHtml,
		InnerText,
		Expressions
	}
}
