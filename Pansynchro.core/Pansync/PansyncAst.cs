using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core.Pansync
{
    public abstract class PansyncNode
    {
        internal void Print(StringBuilder sb, int indentation)
        {
            sb.Append('\t', indentation);
            DoPrint(sb, indentation);
        }

        abstract protected void DoPrint(StringBuilder sb, int indentation);
        abstract protected bool DoesMatch(PansyncNode other);

        public bool Matches(PansyncNode other)
        {
            if (other == null) return false;
            if (other.GetType() != this.GetType()) return false;
            return DoesMatch(other);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            Print(sb, 0);
            return sb.ToString();
        }
    }

    public abstract class Expression : PansyncNode
    { }

    public class NameNode : Expression
    {
        public string Name { get; }
        
        public NameNode(string name)
        {
            Name = name;
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            throw new NotImplementedException();
        }

        protected override bool DoesMatch(PansyncNode other) => ((NameNode)other).Name == Name;

        public override string ToString() => Name;
    }

    public class StringNode : Expression
    {
        public string Value { get; }

        public StringNode(string value)
        {
            Value = value;
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            throw new NotImplementedException();
        }

        protected override bool DoesMatch(PansyncNode other) => ((StringNode)other).Value == Value;

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append('"');
            WriteStringData(Value, sb);
            sb.Append('"');
            return sb.ToString();
        }

        public static void WriteStringData(string text, StringBuilder builder)
        {
            foreach (char ch in text) {
                switch (ch) {
                    case '\r':
                        builder.Append("\\r");
                        break;
                    case '\n':
                        builder.Append("\\n");
                        break;
                    case '\\':
                        builder.Append("\\\\");
                        break;
                    case '"':
                        builder.Append("\\\"");
                        break;
                    default:
                        builder.Append(ch);
                        break;
                }
            }
        }

    }

    public class IntegerNode : Expression
    {
        public long Value { get; }

        public IntegerNode(long value)
        {
            Value = value;
        }

        protected override bool DoesMatch(PansyncNode other) => Value == ((IntegerNode)other).Value;

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            sb.Append(Value);
        }
    }

    public class NamedListNode : Expression
    {
        public NameNode Name { get; }
        public Expression[] Arguments { get; }

        public NamedListNode(NameNode name, Expression[] arguments)
        {
            Name = name;
            Arguments = arguments;
        }

        protected override bool DoesMatch(PansyncNode other)
        {
            var nln = (NamedListNode)other;
            if (!Name.Matches(nln.Name)) return false;
            if (Arguments.Length != nln.Arguments.Length) return false;
            return Arguments.Zip(nln.Arguments).All(z => z.First.Matches(z.Second));
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            sb.Append(Name.ToString());
            sb.Append('(');
            sb.AppendJoin<Expression>(", ", Arguments);
            sb.Append(')');
        }
    }

    public class KvListNode : Expression
    {
        public KeyValuePair<string, Expression>[] Values { get; }

        public KvListNode(KeyValuePair<string, Expression>[] values)
        {
            Values = values;
        }

        protected override bool DoesMatch(PansyncNode other)
        {
            var kvl = (KvListNode)other;
            if (Values.Length != kvl.Values.Length) return false;
            return (Values.Zip(kvl.Values).All(z => z.First.Key == z.Second.Key && z.First.Value.Matches(z.Second.Value)));
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            sb.Append("{ ");
            sb.AppendJoin(", ", Values.Select(kv => $"'{kv.Key}': {kv.Value}"));
            sb.Append(" }");
        }
    }

    public abstract class Statement : PansyncNode { }

    public class Command : Statement
    {
        public string Name { get; }
        public Expression[] Arguments { get; }
        public Expression[]? Results { get; }
        public Statement[]? Body { get; }

        public Command(string name, Expression[] arguments = null!, Expression[]? results = null, Statement[]? body = null)
        {
            Name = name;
            Arguments = arguments ?? Array.Empty<Expression>();
            Results = results;
            Body = body;
        }

        protected override bool DoesMatch(PansyncNode other)
        {
            var ost = (Command)other;
            if (Name != ost.Name) return false;
            if (Arguments.Length != ost.Arguments.Length) return false;
            if (!Arguments.Zip(ost.Arguments).All(z => z.First.Matches(z.Second))) return false;
            if (Results?.Length != ost.Results?.Length) return false;
            if (Results != null && !Results.Zip(ost.Results!).All(z => z.First.Matches(z.Second))) return false;
            if (Body?.Length != ost.Body?.Length) return false;
            if (Body != null && !Body.Zip(ost.Body!).All(z => z.First.Matches(z.Second))) return false;
            return true;
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            sb.Append(Name);
            if (Arguments.Length != 0) {
                sb.Append(' ');
                sb.AppendJoin<Expression>(", ", Arguments);
            }
            if (Results?.Length > 0) {
                sb.Append(" as ");
                sb.AppendJoin<Expression>(", ", Results);
            }
            if (Body?.Length > 0) {
                sb.AppendLine(":");
                foreach (var stmt in Body) {
                    stmt.Print(sb, indentation + 1);
                }
            } else {
                sb.AppendLine();
            }
        }
    }

    public class DataListNode: Statement
    {
        public Expression[] Values { get; }

        public DataListNode(Expression[] values)
        {
            Values = values;
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            sb.Append('[');
            sb.AppendJoin<Expression>(", ", Values);
            sb.AppendLine("]");
        }

        protected override bool DoesMatch(PansyncNode other)
        {
            var dl = (DataListNode)other;
            if (Values.Length != dl.Values.Length) return false;
            return Values.Zip(dl.Values!).All(z => z.First.Matches(z.Second));
        }
    }

    public class PansyncFile : PansyncNode
    {
        public Statement[] Body { get; }

        public PansyncFile(Statement[] body)
        {
            Body = body;
        }

        protected override void DoPrint(StringBuilder sb, int indentation)
        {
            foreach (var stmt in Body) {
                stmt.Print(sb, indentation);
            }
        }

        protected override bool DoesMatch(PansyncNode other)
        {
            var oFile = (PansyncFile)other;
            return Body.Zip(oFile.Body).All(z => z.First.Matches(z.Second));
        }
    }
}
