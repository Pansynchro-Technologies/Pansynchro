using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Pansynchro.SimpleUI.Shared
{
    public class ConnectorConfiguration
    {
        [Required]
        public string? Name { get; set; }

        public ConfigValue[] Responses { get; set; } = Array.Empty<ConfigValue>();

        public void Initialize(ConnectorData? data)
        {
            Name = data?.Name;
            if (data != null) {
                var config = data.Config;
                Responses = config.Select(CreateConfigValue).ToArray();
            }
            else {
                Responses = Array.Empty<ConfigValue>();
            }
        }

        private static ConfigValue CreateConfigValue(ConfigElement elem)
        {
            return elem.Type switch
            {
                ConfigType.Text => new StringConfigValue(elem.Name, elem.Description, elem.Default as string),
                ConfigType.Integer => new IntConfigValue(elem.Name, elem.Description, elem.Default as int? ?? 0),
                ConfigType.Boolean => new BoolConfigValue(elem.Name, elem.Description, elem.Default as bool? ?? false),
                ConfigType.Picklist => new PicklistConfigValue(elem.Name, elem.Description, elem.Options!),
                _ => throw new ArgumentException($"Unknown config element type {elem.Type}")
            };
        }
    }

    public abstract class ConfigValue
    {
        public string Name { get; set; }

        [JsonIgnore]
        public string Description { get; set; }

        public object? Value { get; set; }

        [JsonIgnore]
        public abstract ConfigType Type { get; }

        protected ConfigValue(string name, string description)
        {
            Name = name;
            Description = description;
        }
    }

    public class StringConfigValue : ConfigValue
    {
        public string sValue { get => (string)Value; set => Value = value; }

        public override ConfigType Type => ConfigType.Text;

        public StringConfigValue(string name, string description, string? value) : base(name, description)
        {
            Value = value ?? "";
        }
    }

    public class IntConfigValue : ConfigValue
    {
        public int iValue { get => Value as int? ?? 0; set => Value = value; }

        public override ConfigType Type => ConfigType.Integer;

        public IntConfigValue(string name, string description, int value) : base(name, description)
        {
            Value = value;
        }
    }

    public class BoolConfigValue : ConfigValue
    {
        public bool bValue { get => Value as bool? ?? false; set => Value = value; }

        public override ConfigType Type => ConfigType.Boolean;

        public BoolConfigValue(string name, string description, bool value) : base(name, description)
        {
            Value = value;
        }
    }

    public class PicklistConfigValue : ConfigValue
    {
        public string sValue { get => Value as string; set => Value = value; }

        public string[] Options;

        public override ConfigType Type => ConfigType.Picklist;

        public PicklistConfigValue(string name, string description, string[] options) : base(name, description)
        {
            Options = options;
        }
    }
}
