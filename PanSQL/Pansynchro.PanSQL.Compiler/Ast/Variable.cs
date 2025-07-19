namespace Pansynchro.PanSQL.Compiler.Ast
{
	public record Variable(string Name, string Type, Node Declaration)
	{
		public bool Used { get; set; } = false;
	}
}
