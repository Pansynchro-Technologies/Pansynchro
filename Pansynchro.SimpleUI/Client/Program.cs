using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Pansynchro.SimpleUI;
using Pansynchro.SimpleUI.Shared;
using System.Net.Http.Json;

namespace Company.WebApplication1
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = WebAssemblyHostBuilder.CreateDefault(args);
            builder.RootComponents.Add<App>("#app");
            builder.RootComponents.Add<HeadOutlet>("head::after");

            builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });
            using var http = new HttpClient();
            var profile = await http.GetFromJsonAsync<UserProfile>("https://localhost:7223/api/profile");
            builder.Services.AddSingleton(profile!);

            await builder.Build().RunAsync();
        }
    }
}