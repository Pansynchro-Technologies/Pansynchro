using System;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Protocol;

namespace Pansynchro.NetworkClient
{
	public class PansynchroClient
	{
		public async static Task<IReader> Run(string endpoint, DataDictionary dict)
		{
			try {
				Console.WriteLine($"{DateTime.Now} Attempting to connect to {endpoint}");
				var client = new TcpClient(endpoint, NetworkInfo.TCP_PORT);
				client.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAlive, true);
				client.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 1);
				var stream = client.GetStream();
				Console.WriteLine($"{DateTime.Now} Connected, sending handshake");
				await SendHandshake(stream);
				Console.WriteLine($"{DateTime.Now} Handshake successful");
				return new BinaryDecoder(client, dict);
			} catch (Exception e) {
				Console.WriteLine(e);
				throw;
			}
		}

		private static readonly byte[] KEY_BYTE = new byte[] { 16 };

		[SuppressMessage("Security", "CA5351:Do Not Use Broken Cryptographic Algorithms", Justification = "Not crypto.")]
		private static async Task SendHandshake(NetworkStream stream)
		{
			await stream.WriteAsync(KEY_BYTE);
			var gBuffer = new byte[36];
			await stream.ReadExactlyAsync(gBuffer);
			var gs = Encoding.UTF8.GetString(gBuffer);
			var g = Guid.Parse(gs);
			var hash = MD5.HashData(Encoding.UTF8.GetBytes($"PANSYNCHRO.{g}"));
			await stream.WriteAsync(hash);
		}
	}
}
