using System;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Pansynchro.Core
{
    public class CustomConfiguratorBase : DbConnectionStringBuilder, INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        [AllowNull]
        public override object this[string key]
        {
            get => base[key];
            set {
                TryGetValue(key, out var existing);
                if (existing == null) {
                    if (value == null) {
                        return;
                    }
                }
                else if (existing.Equals(value)) {
                    return;
                }
                base[key] = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(key));
            }
        }

        protected string? GetString(string key) => TryGetValue(key, out var value) ? (string)value : null;
        protected bool GetBool(string key) => TryGetValue(key, out var value) ? Convert.ToBoolean(value) : false;
        protected int GetInt(string key) => TryGetValue(key, out var value) ? Convert.ToInt32(value) : 0;
        
        protected char GetChar(string key, char defaultValue)
        {
            var result = GetString(key);
            return string.IsNullOrEmpty(result) ? defaultValue : result[0];
        }

        protected T GetEnum<T>(string key, T defaultValue) where T : struct, Enum
        {
            var value = GetString(key);
            if (value != null && Enum.TryParse<T>(value, out var result))
                return result;
            return defaultValue;
        }
    }
}
