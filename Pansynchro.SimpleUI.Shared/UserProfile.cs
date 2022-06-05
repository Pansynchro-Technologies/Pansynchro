using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Pansynchro.SimpleUI.Shared
{
    [Flags]
    public enum UserFlags
    {
        None = 0,
        Subscribed = 1,
    }

    public class UserProfile
    { 
        public Guid Id { get; set; }
        public string? Name { get; set; }
        public string? Email { get; set; }
        public UserFlags Flags { get; set; }

        [JsonIgnore]
        public bool IsSubscribed { 
            get => Flags.HasFlag(UserFlags.Subscribed);
            set {
                if (value)
                    Flags |= UserFlags.Subscribed;
                else Flags &= ~UserFlags.Subscribed;
            }
        }
    }
}
