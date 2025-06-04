using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Pansynchro.Core;

namespace Pansynchro.Connectors.FileSystem;
public class FileSystemConfigurator : CustomConfiguratorBase
{
	public FileSystemConfigurator() { }

	public FileSystemConfigurator(string connectionString) : this()
	{
		this.ConnectionString = connectionString;
	}

	[Category("FileSystem")]
	[Description("The folder to list files in. (Required)")]
	public string? WorkingPath
	{
		get => GetString(nameof(WorkingPath));
		set => this[nameof(WorkingPath)] = value;
	}

	[Category("FileSystem")]
	[Description("A filename mask specifying which files to list.")]
	public string? Pattern
	{
		get => GetString(nameof(Pattern));
		set => this[nameof(Pattern)] = value;
	}

	[Category("FileSystem")]
	[Description("If true, search through subdirectories in addition to the WorkingPath.")]
	[DefaultValue(false)]
	public bool Recursive
	{
		get => GetBool(nameof(Recursive));
		set => this[nameof(Recursive)] = value;
	}
}
