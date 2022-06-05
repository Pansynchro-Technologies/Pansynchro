using System.IO;
using System.Text.Json;

using Microsoft.AspNetCore.Mvc;

using Pansynchro.SimpleUI.Shared;

namespace Pansynchro.SimpleUI.Server.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProfileController : ControllerBase
    {
        private static readonly string PROFILE_FILENAME = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "Pansynchro",
            "SimpleUI",
            "profile.json");

        private UserProfile _user = LoadProfile();

        [HttpGet]
        public UserProfile Get() => _user;

        [HttpPost]
        public async Task<Guid> Post(UserProfile data)
        {
            if (data.Id == Guid.Empty) {
                data.Id = Guid.NewGuid();
            }
            await SaveUserChange(data);
            _user = data;
            return data.Id;
        }

        private async Task SaveUserChange(UserProfile data)
        {
            var saveData = JsonSerializer.Serialize(data);
            var submission = (data.Id != _user.Id || data.Email != _user.Email || data.IsSubscribed != _user.IsSubscribed)
                ? SubmitUserChange(data)
                : Task.CompletedTask;
            var save = System.IO.File.WriteAllTextAsync(PROFILE_FILENAME, saveData);
            await Task.WhenAll(save, submission);
        }

        private Task SubmitUserChange(UserProfile data) => Task.CompletedTask; //TODO: implement registration

        private static UserProfile LoadProfile()
        {
            if (System.IO.File.Exists(PROFILE_FILENAME)) {
                return JsonSerializer.Deserialize<UserProfile>(System.IO.File.ReadAllText(PROFILE_FILENAME))
                    ?? new();
            }
            return new();
        }
    }
}
