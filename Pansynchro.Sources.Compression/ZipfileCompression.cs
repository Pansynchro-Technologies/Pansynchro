using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Pansynchro.Core;

namespace Pansynchro.Sources.Compression;
public class ZipfileCompression : IDataInputProcessor, IDataOutputProcessor
{
	private IDataSource? _source;

	public void SetDataSource(IDataSource source) => _source = source;

	public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
	{
		if (_source == null) {
			throw new DataException("Must call SetDataSource before calling GetDataAsync");
		}
		await foreach (var (name, data) in _source.GetDataAsync()) {
			using var zip = new ZipArchive(data);
			foreach (var file in zip.Entries) {
				using var stream = file.Open();
				yield return (file.FullName, stream);
			}
		}
	}

	public async IAsyncEnumerable<Stream> GetDataAsync(string name)
	{
		if (_source == null) {
			throw new DataException("Must call SetDataSource before calling GetDataAsync");
		}
		await foreach (var data in _source.GetDataAsync(name)) {
			using var zip = new ZipArchive(data);
			foreach (var file in zip.Entries) {
				using var stream = file.Open();
				yield return stream;
			}
		}
	}

	public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
	{
		await foreach (var (name, stream) in GetDataAsync()) {
			yield return (name, new StreamReader(stream));
		}
	}

	public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
	{
		await foreach (var stream in GetDataAsync(name)) {
			yield return new StreamReader(stream);
		}
	}

	private IDataSink? _sink;

	public void SetDataSink(IDataSink sink) => _sink = sink;

	private ZipArchive? _currentArchive;
	private JsonObject? _config;

	public ZipfileCompression(string config) => _config = (JsonNode.Parse(config) as JsonObject);

	private async ValueTask<ZipArchive> GetArchive()
	{
		if (_sink == null) {
			throw new DataException("Must call SetDataSink before calling WriteData.");
		}
		var name = (string?)_config?["StreamName"];
		if (name == null) {
			throw new DataException("Config is missing StreamName, or it is invalid.");
		}
		if (_currentArchive == null) {
			var stream = await _sink.WriteData(name);
			_currentArchive = new ZipArchive(stream, ZipArchiveMode.Create, false);
		}
		return _currentArchive;
	}

	public async Task<Stream> WriteData(string streamName)
	{
		var arc = await GetArchive();
		var file = arc.CreateEntry(streamName);
		return file.Open();
	}

	public void Dispose()
	{
		_currentArchive?.Dispose();
		(_source as IDisposable)?.Dispose();
		(_sink as IDisposable)?.Dispose();
		GC.SuppressFinalize(this);
	}
}
