using Pansynchro.Core.DataDict;

namespace Pansynchro.PanSQL.Compiler.DataBuiltin;
internal static class FileSystem
{
	public static DataDictionary Dict { get; } = DataDictionaryWriter.Parse(@"DataDictionary FileSystem:
	Stream Files:
		Field FullName(Ntext NOT NULL)
		Field Name(Ntext NOT NULL)
		Field CreationTime(DateTime NOT NULL)
		Field LastModifiedTime(DateTime NOT NULL)
		Field Extension(Ntext NOT NULL)
		Field Size(Long NOT NULL)
		Identity FullName
	DependencyOrder :
		[Files]
	CustomTypes
");
}
