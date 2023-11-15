using Pansynchro.PanSQL.Compiler.DataModels;

namespace Pansynchro.PanSQL.Compiler
{
	public class Script
	{
		private string _result = null!;

		public string Code => _result;
		public string ProjectFile { get; internal set; } = "";
		public string Name { get; internal set; } = null!;
		public string Connectors { get; internal set; } = "";

		internal void SetFile(FileModel file)
		{
			_result = file.Serialize();
		}
	}
}
