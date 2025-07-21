using System.IO;
using System.Net.Http;
using System.Text.Json.Nodes;

namespace Pansynchro.PanSQL.Core;
public static partial class SqlFunctions
{
	public static string HttpQuery(string url)
	{
		using var client = new HttpClient();
		return client.GetStringAsync(url).GetAwaiter().GetResult();
	}

	public static JsonNode HttpQueryJson(string url) => JsonNode.Parse(HttpQuery(url))!;

	public static string HttpPost(string url, string content)
	{
		using var client = new HttpClient();
		var result = client.PostAsync(url, new StringContent(content)).GetAwaiter().GetResult();
		return result.Content.ReadAsStringAsync().GetAwaiter().GetResult();
	}

	public static JsonNode HttpPostJson(string url, string content) => JsonNode.Parse(HttpPost(url, content))!;

	private static MultipartFormDataContent GetFileData(string filename)
	{
		var result = new MultipartFormDataContent();
		var content = new ByteArrayContent(File.ReadAllBytes(filename));
		result.Add(content, Path.GetFileName(filename));
		return result;
	}

	public static string HttpPostFile(string url, string filename)
	{
		using var client = new HttpClient();
		using var form = GetFileData(filename);
		var result = client.PostAsync(url, form).GetAwaiter().GetResult();
		return result.Content.ReadAsStringAsync().GetAwaiter().GetResult();
	}

	public static string HttpPutFile(string url, string filename)
	{
		using var client = new HttpClient();
		using var form = GetFileData(filename);
		var result = client.PutAsync(url, form).GetAwaiter().GetResult();
		return result.Content.ReadAsStringAsync().GetAwaiter().GetResult();
	}
}
