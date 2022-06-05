using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Pansynchro.Connectors.TextFile.JSON
{
    class JsonConfigurator : DbConnectionStringBuilder, INotifyPropertyChanged
    {

        public ObservableCollection<JsonConf> Streams { get; set; } = new();

        public JsonConfigurator()
        {
            Streams.CollectionChanged += this.DictChanged;
            DictChanged(null, null);
        }

        public JsonConfigurator(string conf) : this()
        {
            ConnectionString = conf;
        }

        public event PropertyChangedEventHandler? PropertyChanged
        {
            add {
                ((INotifyPropertyChanged)Streams).PropertyChanged += value;
            }
            remove {
                ((INotifyPropertyChanged)Streams).PropertyChanged -= value;
            }
        }

        private void DictChanged(object? sender, NotifyCollectionChangedEventArgs? e)
        {
            ConnectionString = string.Join(';',
                Streams.Select(s => $"{s.Name}={JsonConvert.SerializeObject(s)}"));
        }

        public override object this[string keyword] {
            get => keyword == "Streams" ? Streams : base[keyword];
        }

        public override bool TryGetValue(string keyword, [NotNullWhen(true)] out object? value)
        {
            if (keyword == "Streams") {
                value = Streams;
                return true;
            }
            return base.TryGetValue(keyword, out value);
        }
    }

    public class JsonConf : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        public JsonConf() { }

        private string _name = "New Stream";
        [Category("JSON")]
        [Description("Name of the stream.  Each stream should have its own unique name.")]
        public string Name {
            get => _name;
            set {
                _name = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
            }
        }

        private FileType _fileStructure;
        [Category("JSON")]
        [Description("How should the top-level format of the data be interpreted?")]
        [DefaultValue(FileType.Array)]
        public FileType FileStructure
        {
            get => _fileStructure;
            set {
                _fileStructure = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(FileStructure)));
            }
        }

        private string? _schema;
        [Category("JSON")]
        [Description("The location of a JSON schema file describing this data")]
        public string? Schema
        {
            get => _schema;
            set {
                _schema = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Schema)));
            }
        }

        [Category("JSON")]
        [Description("A list of JSONPath expressions that contain relevant data streams")]
        public ObservableCollection<JsonQuery> Streams { get; set; } = new();

        private string? _errorPath;
        [Category("JSON")]
        [Description("A JSONPath expression containing error information. If this finds a value, the sync will raise the error.")]
        public string? ErrorPath
        {
            get => _errorPath;
            set {
                _errorPath = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(ErrorPath)));
            }
        }

        public override string ToString() => Name;
    }

    public enum FileType
    {
        Array,
        Obj
    }

    public class JsonQuery : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        public JsonQuery() { }

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
        [Description("JSONPath expression to extract the relevant data stream")]
        public string? Path {
            get => _path;
            set {
                _path = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Path)));
            }
        }

        private bool _required;
        [Description("If true, the sync will fail with an error if the Path query fails to select any data.")]
        public bool Required {
            get => _required;
            set {
                _required = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Required)));
            }
        }
    }
}
