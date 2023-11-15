using System.IO;

namespace Pansynchro.PanSQL.Core
{
	public static class FilenameHelper
	{
		public static string Normalize(string filename)
		{
			if (filename.Contains('\\') && '\\' != Path.DirectorySeparatorChar) {
				filename = filename.Replace('\\', '/');
			}
			if (filename.Contains('/') && '/' != Path.DirectorySeparatorChar) {
				filename = filename.Replace('/', '\\');
			}
			return filename;
		}
	}
}
