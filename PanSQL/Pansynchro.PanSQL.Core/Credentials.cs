using System;

namespace Pansynchro.PanSQL.Core
{
	public static class Credentials
	{
		public static string CredentialsFromEnv(string key) => Environment.GetEnvironmentVariable(key) 
			?? throw new ArgumentException($"No environment variable named '{key}' has been set.");
	}
}
