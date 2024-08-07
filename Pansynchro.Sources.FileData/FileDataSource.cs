﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

using Pansynchro.Core;

namespace Pansynchro.Sources.Files
{
	record FileSpec(string Name, string[] File);

	public class FileDataSource : IDataSource
	{
		private readonly FileSpec[] _conn;
		private readonly int? _textBufferSize;

		public FileDataSource(string connectionString)
		{
			var json = JObject.Parse(connectionString);
			var files = json["Files"] as JArray;
			if (files == null) {
				throw new ArgumentException("Config JSON is missing the Files property");
			}
			var list = files.ToObject<FileSpec[]>();
			if (list == null) {
				throw new ArgumentException("JSON Files property does not match the spec");
			}
			_conn = list;
			if (json.ContainsKey("TextBufferSize")) {
				_textBufferSize = (int)json["TextBufferSize"]!;
			}
		}

		private IEnumerable<(string name, string filename)> GetFilenames()
		{
			foreach (var (name, specs) in _conn) {
				foreach (var spec in specs) {
					foreach (var filename in GetFiles(spec)) {
						yield return (name, filename);
					}
				}
			}
		}

		private IEnumerable<string> GetFiles(string spec) => new GlobFileScanner(spec).Files;

		public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
		{
			foreach (var (name, filename) in GetFilenames()) {
				yield return (name.Replace("*", Path.GetFileNameWithoutExtension(filename)), File.OpenRead(filename));
			}
			await Task.CompletedTask; //just here to shut the compiler up
		}

		public async IAsyncEnumerable<Stream> GetDataAsync(string name)
		{
			foreach (var (lName, filename) in GetFilenames()) {
				if (lName == name) {
					yield return File.OpenRead(filename);
				}
			}
			await Task.CompletedTask;  //just here to shut the compiler up
		}

		public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
		{
			await foreach (var (name, stream) in GetDataAsync()) {
				yield return (name, _textBufferSize == null ? new StreamReader(stream) : new StreamReader(stream, bufferSize: _textBufferSize.Value));
			}
		}

		public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
		{
			await foreach (var stream in GetDataAsync(name)) {
				yield return _textBufferSize == null ? new StreamReader(stream) : new StreamReader(stream, bufferSize: _textBufferSize.Value);
			}
		}
	}
}
