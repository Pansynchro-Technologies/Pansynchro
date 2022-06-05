using System.ComponentModel;
using System.Data.Common;
using System.Linq;

namespace Pansynchro.Core.Connectors
{
    public class SimpleConnectionStringBuilder : INotifyPropertyChanged
    {
        private readonly DbConnectionStringBuilder _inner;
        private readonly PropertyDescriptor _userProp;
        private readonly PropertyDescriptor _passProp;
        private readonly PropertyDescriptor _serverProp;
        private readonly PropertyDescriptor _dbProp;

        public event PropertyChangedEventHandler? PropertyChanged;

        public string? ServerLocation
        {
            get => (string?)_serverProp.GetValue(_inner);
            set {
                _serverProp.SetValue(_inner, value);
                RaisePropertyChanged(nameof(ServerLocation));
            }
        }

        public string? DatabaseName
        {
            get => (string?)_dbProp.GetValue(_inner);
            set {
                _dbProp.SetValue(_inner, value);
                RaisePropertyChanged(nameof(DatabaseName));
            }
        }

        public string? Username
        {
            get => (string?)_userProp.GetValue(_inner);
            set {
                _userProp.SetValue(_inner, value);
                RaisePropertyChanged(nameof(Username));
            }
        }

        public string? Password
        {
            get => (string?)_passProp.GetValue(_inner);
            set {
                _passProp.SetValue(_inner, value);
                RaisePropertyChanged(nameof(Password));
            }
        }

        public string ConnectionString
        {
            get => _inner.ConnectionString;
            set {
                _inner.ConnectionString = value;
                RaisePropertyChanged(null);
            }
        }

        public SimpleConnectionStringBuilder(
            DbConnectionStringBuilder inner, 
            string userProp, string passProp, string serverProp, string dbProp)
        {
            _inner = inner;
            var props = ((ICustomTypeDescriptor)_inner).GetProperties()
                .Cast<PropertyDescriptor>()
                .ToDictionary(p => p.Name);
            _userProp = props[userProp];
            _passProp = props[passProp];
            _serverProp = props[serverProp];
            _dbProp = props[dbProp];
        }

        private void RaisePropertyChanged(string? name)
        {
            PropertyChanged?.Invoke(this, new(name));
        }
    }
}