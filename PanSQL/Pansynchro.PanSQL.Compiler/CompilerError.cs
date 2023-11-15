using System;

using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler
{
	public class CompilerError : Exception
	{
		public CompilerError(Node node)
		{
			Node = node;
		}

		public CompilerError(string? message, Node node) : base(message)
		{
			Node = node;
		}

		public CompilerError(string? message, Exception? innerException, Node node) : base(message, innerException)
		{
			Node = node;
		}

		public Node Node { get; }
	}
}