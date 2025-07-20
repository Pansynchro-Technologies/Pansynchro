using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.Protocol;

namespace Pansynchro.NetworkServer
{
	class PansynchroServer
	{
		static async Task Main(string[] args)
		{
			if (args.Length != 1) {
				Console.WriteLine("Usage: Pansynchro.NetworkServer [configFileName]");
				Console.WriteLine($"Args: {string.Join(' ', args)}");
				return;
			}
			var configTxt = File.ReadAllText(args[0]);
			var config = JsonSerializer.Deserialize<ServerConfig>(configTxt)!;
			config.Validate();
			var server = new TcpListener(IPAddress.Any, NetworkInfo.TCP_PORT);
			server.Start();
			try {
				Console.WriteLine($"{DateTime.Now}: Server started.");
				using var client = await GetClientConnection(server);
				Console.WriteLine($"{DateTime.Now}: Connection successful");
				await RunSync(config, server, client);
			} finally {
				server.Stop();
			}
		}

		private static async Task RunSync(ServerConfig config, TcpListener server, TcpClient client)
		{
			var sw = new Stopwatch();
			sw.Start();
			using Stream stream = config.Certificate != null
				? GetSslStream(client, config.Certificate, config.Thumbprint)
				: client.GetStream();
			var dict = DataDictionary.LoadFromFile(config.DataDict);
			using IReader reader = ConnectorRegistry.GetReader(config.InputType, config.ConnectionString);
			var encoder = new BinaryEncoder(stream);
			try {
				await encoder.Sync(reader.ReadFrom(dict), dict);
			} catch (IOException x) {
				HandleIOException(x);
				throw;
			}
			sw.Stop();
			Console.WriteLine($"{DateTime.Now}: Sync finished in {sw.Elapsed}.");
		}

		private static void HandleIOException(IOException x)
		{
			if (x.InnerException is SocketException socket) {
				Console.WriteLine($"Socket exception occurred: {socket.SocketErrorCode} ({socket.ErrorCode})");
			}
		}

		private static SslStream GetSslStream(TcpClient client, string certificateFile, string[]? thumbprint)
		{
			var c = X509Certificate.CreateFromCertFile(certificateFile);
			var cert = new X509Certificate2(c);
			if (!cert.Verify()) {
				throw new SecurityException("Certificate failed verification");
			}
			var stream = new SslStream(client.GetStream(), false, VerifyClientCert(thumbprint?.ToHashSet()));
			stream.AuthenticateAsServer(cert, true, true);
			return stream;
		}

		private static RemoteCertificateValidationCallback VerifyClientCert(HashSet<string>? thumbprint)
		{
			return (sender, clientCert, chain, errors) => {
				if (clientCert == null) {
					return false;
				}
				var cert = new X509Certificate2(clientCert);
				if (!cert.Verify()) {
					return false;
				}
				return thumbprint == null || thumbprint.Contains(cert.Thumbprint);
			};
		}

		private static async Task<TcpClient> GetClientConnection(TcpListener server)
		{
			using var cts = new CancellationTokenSource();
			TcpClient? result = null;
			while (result == null) {
				try {
					var client = await server.AcceptTcpClientAsync(cts.Token);
					Console.WriteLine($"{DateTime.Now}: Client connected: {client.Client.RemoteEndPoint} / {client.Client.LocalEndPoint}");
					_ = Task.Run(async () => {
						if (await ValidateHandshake(client, cts.Token)) {
							result = client;
							cts.Cancel();
						} else {
							Console.WriteLine($"{DateTime.Now}: Handshake failed");
							client.Dispose();
						}
					});
				} catch (OperationCanceledException) { }
			}
			return result;
		}

		private const byte KEY_BYTE = 16;

		[SuppressMessage("Security", "CA5351:Do Not Use Broken Cryptographic Algorithms", Justification = "Not crypto.")]
		private static async Task<bool> ValidateHandshake(TcpClient client, CancellationToken token)
		{
			var stream = client.GetStream();
			using var cts = new CancellationTokenSource(1500);
			using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, cts.Token);
			var kBuffer = new byte[1];
			try {
				await stream.ReadAsync(kBuffer, linked.Token);
				if (kBuffer[0] != KEY_BYTE) {
					return false;
				}
				var guid = Guid.NewGuid();
				var gBuffer = Encoding.UTF8.GetBytes(guid.ToString());
				var hBuffer = new byte[16];
				await stream.WriteAsync(gBuffer, linked.Token);
				await stream.ReadAsync(hBuffer, linked.Token);
				var hash = MD5.HashData(Encoding.UTF8.GetBytes($"PANSYNCHRO.{guid}"));
				if (hBuffer.SequenceEqual(hash)) {
					return true;
				} else {
					Console.WriteLine($"Expected: {string.Join(' ', hash)}, Received: {string.Join(' ', hBuffer)}");
					return false;
				}
			} catch (OperationCanceledException) {
				Console.WriteLine($"Handshake timeout");
				return false;
			}
		}
	}
}
