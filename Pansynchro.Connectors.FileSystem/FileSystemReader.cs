using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.FileSystem;

public class FileSystemReader : IReader
{
	private readonly FileSystemConfigurator _configurator;

	public FileSystemReader(string config)
	{
		_configurator = new FileSystemConfigurator(config);
		if (!_configurator.ContainsKey("WorkingPath")) {
			throw new DataException("A WorkingPath value must be set to use the FileSystem reader.");
		}
	}

	public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
	{
		if (!source.Equals(FileSystemAnalyzer.Dict)) {
			throw new DataException("The FileSystem reader is only compatible with the FileSytem data dictionary.");
		}
		var stream = source.GetStream("Files");
		yield return new DataStream(stream.Name, StreamSettings.None, new FileSystemDataReader(_configurator));
		await Task.CompletedTask; //just to shut the compiler up
	}

	public Task<Exception?> TestConnection() => Task.FromResult<Exception?>(null);
	
	public void Dispose() => GC.SuppressFinalize(this);
}
