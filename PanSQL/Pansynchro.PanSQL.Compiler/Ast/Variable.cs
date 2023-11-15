namespace Pansynchro.PanSQL.Compiler.Ast
{
	public record Variable(string Name, string Type, Statement Declaration)
	{
		public bool Used { get; set; } = false;
	}
}
