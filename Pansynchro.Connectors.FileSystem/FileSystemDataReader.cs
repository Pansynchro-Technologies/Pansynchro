using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.FileSystem;
internal class FileSystemDataReader : ArrayReader
{
	public FileSystemDataReader(FileSystemConfigurator configurator)
	{
		this._buffer = new object[6];
		foreach(var pair in _names.Select(KeyValuePair.Create)) {
			_nameMap.Add(pair.Key, pair.Value);
		}
		LoadFileInfo(configurator.WorkingPath!, configurator.Pattern, configurator.Recursive);
	}

	private void LoadFileInfo(string workingPath, string? pattern, bool recursive)
	{
		var dir = new DirectoryInfo(workingPath);
		if (!dir.Exists) {
			return;
		}
		_files.AddRange(dir.GetFileSystemInfos(pattern ?? "*.*", recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly).OfType<FileInfo>());
	}

	private List<FileInfo> _files = [];
	public override int RecordsAffected => _files.Count;

	private string[] _names = ["FullName", "Name", "CreationTime", "LastModifiedTime", "Extension", "Size"];

	public override string GetName(int i) => _names[i];

	private int _idx = -1;

	public override bool Read()
	{
		++_idx;
		if (_idx >= _files.Count) {
			return false;
		}
		var info = _files[_idx];
		_buffer[0] = info.FullName;
		_buffer[1] = info.Name;
		_buffer[2] = info.CreationTime;
		_buffer[3] = info.LastWriteTime;
		_buffer[4] = info.Extension;
		_buffer[5] = info.Length;
		return true;
	}

	public override void Dispose() => GC.SuppressFinalize(this);
}