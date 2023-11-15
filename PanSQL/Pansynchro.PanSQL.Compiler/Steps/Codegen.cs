using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Pansynchro.Core.Connectors;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class Codegen : VisitorCompileStep
	{
		public Script Output { get; } = new();
		
		private bool _transformer;
		private readonly HashSet<string> _connectorRefs = [];
		private readonly HashSet<string> _connectors = [];
		private readonly HashSet<string> _sources = [];

		private static readonly ImportModel[] USING_BLOCK = [
			"System",
			"System.Collections.Generic",
			"System.Data",
			"System.Threading.Tasks",
			"Pansynchro.Core",
			"Pansynchro.Core.DataDict",
			"Pansynchro.Core.Connectors",
			"Pansynchro.PanSQL.Core",
			new ("Pansynchro.PanSQL.Core.Credentials", true)];

		private static readonly ImportModel[] USING_DB = [
			"NMemory",
			"NMemory.Tables",
			"NMemory.Indexes"];

		private const string CSPROJ = @"<Project Sdk=""Microsoft.NET.Sdk"">

	<PropertyGroup>
		<OutputType>Exe</OutputType>
		<TargetFramework>net8.0</TargetFramework>
		<Nullable>enable</Nullable>
		<CopyLocalLockFileAssemblies>true</CopyLocalLockFileAssemblies>
		<NoWarn>$(NoWarn);CS8621</NoWarn>
	</PropertyGroup>

	<ItemGroup>
		<PackageReference Include=""NMemory"" Version=""*"" />
		<PackageReference Include=""Pansynchro.Core"" Version=""*"" />
		<PackageReference Include=""Pansynchro.PanSQL.Core"" Version=""*"" />
		{0}
	</ItemGroup>

	<ItemGroup>
		<None Update=""connectors.pansync"">
			<CopyToOutputDirectory>Always</CopyToOutputDirectory>
		</None>
	</ItemGroup>
	
</Project>
";
		public override void Execute(PanSqlFile f)
		{
			base.Execute(f);
			var connectorRefs = string.Join(Environment.NewLine + "\t\t", _connectorRefs.Select(BuildConnectorReference));
			Output.ProjectFile = string.Format(CSPROJ, connectorRefs);
			Output.Connectors =
				ConnectorRegistry.WriteConnectors([.. _connectors.Order()], _sources.Count > 0 ? [.. _sources.Order()] : null);
		}

		private string BuildConnectorReference(string source)
		{
			var asm = ConnectorRegistry.GetLocation(source);
			return $"<PackageReference Include=\"{asm}\" Version=\"*\" />";
		}

		private List<CSharpStatement> _mainBody = null!;

		public override void OnFile(PanSqlFile node)
		{
			var imports = new List<ImportModel>(USING_BLOCK);
			if (node.Database.Count > 0) {
				imports.AddRange(USING_DB);
			}
			_transformer = _file.Mappings.Count != 0;
			var classes = new List<ClassModel>();
			if (_transformer) {
				classes.Add(BuildTransformer(node, imports));
			}
			_mainBody = [];
			base.OnFile(node);
			var main = new Method("public static async", "Main", "Task", null, _mainBody);
			classes.Add(new("static", "Program", null, null, [], [main]));
			var result = new FileModel(SortImports(imports).ToArray(), [.. classes]);
			Output.SetFile(result);
		}

		private static IEnumerable<ImportModel?> SortImports(List<ImportModel> imports)
		{
			var groups = imports.DistinctBy(i => i.Name).ToLookup(i => i.Name.Split('.')[0]);
			foreach (var group in groups) {
				foreach (var item in group.OrderBy(i => i.Name)) {
					yield return item;
				}
				yield return null;
			}
		}

		private ClassModel BuildTransformer(PanSqlFile node, List<ImportModel> imports)
		{
			var fields = new List<DataFieldModel>();
			var methods = new List<Method>();
			methods.AddRange(_file.Lines.OfType<VarDeclaration>().Select(GetVarScript).Where(m => m != null)!);
			methods.AddRange(_file.Lines.OfType<SqlTransformStatement>().Select(t => GetSqlScript(t, imports)));
			if (_file.Producers.Count > 0) {
				methods.Add(BuildStreamLast());
			}
			var body = new List<CSharpStatement>();
			foreach (var tf in _file.Transformers) {
				var tableName = ((VarDeclaration)_file.Vars[tf.Key.Name].Declaration).Stream.Name.ToString();
				body.Add(new CSharpStringExpression($"_streamDict.Add({tableName.ToLiteral()}, {tf.Value})"));
			}
			foreach (var pr in _file.Producers) {
				var tableName = ((VarDeclaration)_file.Vars[pr.Key.Name].Declaration).Stream.Name.ToString();
				body.Add(new CSharpStringExpression($"_producers.Add((destDict.GetStream({tableName.ToLiteral()}), {pr.Value}))"));
			}
			foreach (var m in _file.Mappings) {
				if (m.Key != m.Value) {
					body.Add(new CSharpStringExpression($"_nameMap.Add(StreamDescription.Parse({m.Key.ToLiteral()}), StreamDescription.Parse({m.Value.ToLiteral()}))"));
				}
			}
			methods.Add(new Method("public", "Sync", "", "DataDictionary destDict", body, true, "destDict"));
			var subclasses = node.Database.Count > 0 ? new ClassModel[] { GenerateDatabase(node.Database, fields) } : null;
			return new ClassModel("", "Sync", "StreamTransformerBase", subclasses, [.. fields], [.. methods]);
		}

		private static Method BuildStreamLast()
		{
			var body = new List<CSharpStatement>();
			var loopBody = new List<CSharpStatement>() {
				new YieldReturn(new CSharpStringExpression("new DataStream(stream.Name, StreamSettings.None, new ProducingReader(producer(), stream))"))
			};
			body.Add(new ForeachLoop("(stream, producer)", "_producers", loopBody));
			body.Add(new CSharpStringExpression("await Task.CompletedTask"));
			return new Method("public override", "StreamLast", "async IAsyncEnumerable<DataStream>", null, body);
		}

		public override void OnAnalyzeStatement(AnalyzeStatement node)
		{
			string line;
			var list = node.IncludeList;
			if (list != null) {
				line = $"await ((IQueryableSchemaAnalyzer){node.Conn}).AnalyzeAsync(\"{node.Dict}\", [{string.Join(", ", list.Select(l => l.ToString()!.ToLiteral()))}])";
			} else {
				list = node.ExcludeList;
				if (list != null) {
					line = $"await ((IQueryableSchemaAnalyzer){node.Conn}).AnalyzeExcludingAsync(\"{node.Dict}\", [{string.Join(", ", list.Select(l => l.ToString()!.ToLiteral()))}])";
				} else {
					line = $"await {node.Conn}.AnalyzeAsync(\"{node.Dict}\")";
				}
			}
			_mainBody.Add(new VarDecl(node.Dict.Name, new CSharpStringExpression(line)));
			if (node.Optimize) {
				_mainBody.Add(new CSharpStringExpression($"{node.Dict} = await {node.Conn}.Optimize({node.Dict}, _ => {{ }})"));
			}
		}

		private ClassModel GenerateDatabase(List<DataClassModel> database, List<DataFieldModel> outerFields)
		{
			var fields = new List<DataFieldModel>();
			var subclasses = database.Select(m => GenerateModelClass(m, fields)).ToArray();
			var ctor = GenerateDatabaseConstructor(database);
			outerFields.Add(new("__db", "readonly DB", "new()"));
			if (_file.Producers.Count > 0) {
				outerFields.Add(new("_producers", "readonly List<(StreamDefinition stream, Func<IEnumerable<object?[]>> producer)>", "new()"));
			}
			return new ClassModel("private", "DB", "Database", subclasses, [.. fields], [ctor]);
		}

		private static Method GenerateDatabaseConstructor(List<DataClassModel> database)
		{
			var body = new List<CSharpStatement> { new CSharpStringExpression("NMemory.NMemoryManager.DisableObjectCloning = true") };
			foreach (var model in database) {
				GenerateModelConstruction(model, body);
			}
			return new Method("public", "DB", "", null, body, true);
		}

		private static void GenerateModelConstruction(DataClassModel model, List<CSharpStatement> body)
		{
			body.Add(new CSharpStringExpression($"{model.Name[..^1]} = Tables.Create<{model.Name}, {TypesHelper.ModelIdentityType(model)}>(t => {TypesHelper.ModelIdentityFields(model, "t")})"));
			body.Add(new CSharpStringExpression($"{model.Name}{TypesHelper.ModelIdentityName(model)} = (IUniqueIndex<{model.Name}, {TypesHelper.ModelIdentityType(model)}>){model.Name[..^1]}.PrimaryKeyIndex"));
		}

		private static ClassModel GenerateModelClass(DataClassModel model, List<DataFieldModel> outerFields)
		{
			var fields = model.Fields.Select(f => new DataFieldModel(f.Name, f.Type, null, true)).ToArray();
			var lines = new List<CSharpStatement>();
			for (int i = 0; i < model.Fields.Length; ++i) {
				var field = model.Fields[i];
				lines.Add(new CSharpStringExpression($"this.{field.Name} = {string.Format(field.Initializer!, i)}"));
			}
			var ctor = new Method("public", model.Name, "", "IDataReader r", lines, true);
			outerFields.Add(new(model.Name[..^1], $"ITable<{model.Name}>", null, true));
			outerFields.Add(new($"{model.Name}{TypesHelper.ModelIdentityName(model)}", $"IUniqueIndex<{model.Name}, {TypesHelper.ModelIdentityType(model)}>", null, true));
			return new ClassModel("public", model.Name, null, null, fields, [ctor]);
		}

		public override void OnLoadStatement(LoadStatement node)
		{
			var cs = node.Dict.ToString().ToCompressedString();
			_mainBody.Add(new VarDecl(node.Name, new CSharpStringExpression($"DataDictionaryWriter.Parse(CompressionHelper.Decompress({cs.ToLiteral()}))")));
		}

		public override void OnSaveStatement(SaveStatement node)
		{
			_mainBody.Add(new CSharpStringExpression($"{node.Name}.SaveToFile(FilenameHelper.Normalize({node.Filename.ToLiteral()}))"));
		}

		public override void OnVarDeclaration(VarDeclaration node)
		{ }

		public override void OnOpenStatement(OpenStatement node)
		{
			var method = node.Type switch {
				OpenType.Read => "GetReader",
				OpenType.Write => "GetWriter",
				OpenType.Analyze => "GetAnalyzer",
				OpenType.Source => "GetSource",
				OpenType.Sink => "GetSink",
				_ => throw new NotImplementedException(),
			};
			if (node.Connector.Equals("Network", StringComparison.InvariantCultureIgnoreCase)) {
				ProcessNetworkConnection(node);
			}
			var line = $"ConnectorRegistry.{method}({node.Connector.ToLiteral()}, {node.Creds})";
			_mainBody.Add(new VarDecl(node.Name, new CSharpStringExpression(line)));
			if (node.Source != null) {
				line = node.Type switch {
					OpenType.Read or OpenType.Analyze => $"((ISourcedConnector){node.Name}).SetDataSource({node.Source})",
					OpenType.Write => $"((ISinkConnector){node.Name}).SetDataSink({node.Source})",
					_ => throw new NotImplementedException()
				};
				_mainBody.Add(new CSharpStringExpression(line));
			}
			_connectorRefs.Add(node.Connector);
			(node.Type is OpenType.Read or OpenType.Write or OpenType.Analyze ? _connectors : _sources).Add(node.Connector);
		}

		private void ProcessNetworkConnection(OpenStatement node)
		{
			if (node.Creds.Method == "__direct") {
				var dictRef = node.Dictionary.Name;
				var filename = CodeBuilder.NewNameReference("filename");
				_mainBody.Add(new VarDecl(filename.Name, new CSharpStringExpression("System.IO.Path.GetTempFileName()")));
				_mainBody.Add(new CSharpStringExpression($"{dictRef}.SaveToFile({filename})"));
				node.Creds = new CredentialExpression("__literal", $"\"{node.Creds.Value};\" + {filename}"); 
			}
		}

		private Method GetSqlScript(SqlTransformStatement node, List<ImportModel> imports)
		{
			var method = node.DataModel.GetScript(CodeBuilder, node.Indices, imports);
			if (_file.Transformers.ContainsKey(node.Tables[0]))
			{
				_file.Producers.Add(node.Tables[0], method.Name);
			} else { 
				_file.Transformers.Add(node.Tables[0], method.Name);
			}
			return method;
		}

		private Method? GetVarScript(VarDeclaration decl)
		{
			if (decl.Type == VarDeclarationType.Table) {
				var methodName = CodeBuilder.NewNameReference("Transformer");
				_file.Transformers.Add(_file.Vars[decl.Name], methodName.Name);
				var tableName = decl.Stream.Name;
				var body = new List<CSharpStatement>();
				var loopBody = new List<CSharpStatement>() { new CSharpStringExpression($"__db.{tableName}.Insert(new DB.{tableName}_(r))") };
				body.Add(new WhileLoop(new CSharpStringExpression("r.Read()"), loopBody));
				body.Add(new YieldBreak());
				return new Method("private", methodName.Name, "IEnumerable<object?[]>", "IDataReader r", body);
			}
			return null;
		}

		public override void OnSqlStatement(SqlTransformStatement node)
		{ }

		public override void OnMapStatement(MapStatement node)
		{ }

		public override void OnSyncStatement(SyncStatement node)
		{
			var input = _file.Vars[node.Input.Name];
			var inputDict = ((OpenStatement)input.Declaration).Dictionary;
			var inputName = CodeBuilder.NewNameReference("reader");
			var output = _file.Vars[node.Output.Name];
			var outputDict = ((OpenStatement)output.Declaration).Dictionary;
			_mainBody.Add(new VarDecl(inputName.Name, new CSharpStringExpression($"{input.Name}.ReadFrom({inputDict})")));
			if (_transformer) {
				_mainBody.Add(new CSharpStringExpression($"{inputName} = new Sync({outputDict}).Transform({inputName})"));
			}
			_mainBody.Add(new CSharpStringExpression($"await {output.Name}.Sync({inputName}, {outputDict})"));
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies()
			=> [Dependency<TypeCheck>(), Dependency<BuildMappings>(), Dependency<BuildDatabase>()];
	}
}
