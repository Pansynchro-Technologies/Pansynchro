using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using DocoptNet;

using Pansynchro.PanSQL.Compiler;

namespace Pansynchro.PanSQL
{
	internal class Program
	{
		static int ShowHelp(string help) { Console.WriteLine(help); return 0; }
		static int ShowVersion(string version) { Console.WriteLine(version); return 0; }
		static int OnError(string usage) { Console.WriteLine(usage); return 1; }

		static int Run(ProgramArguments args)
		{
			_ = Pansynchro.Core.Connectors.ConnectorRegistry.ReaderTypes.ToArray(); // Hitting this to ensure the connectors load

			var files = args.ArgScriptFilename.ToArray();
			bool multiple = false;
			switch (files.Length) {
				case 0:
					Console.WriteLine("No PanSQL script filename was specified");
					return 1;
				case 1:
					var filename = Path.GetFullPath(files[0]);
					if (filename.IndexOfAny(['?', '*']) == -1) { 
						if (!File.Exists(filename)) {
							Console.WriteLine($"Unable to find '{args.ArgScriptFilename}'");
							return 1;
						}
					} else {
						multiple = true;
					}
					break;
				default:
					multiple = true;
					break;
			}
			return multiple ? CompileMultiple(files, args.OptNobuild, args.OptVerbose) : CompileSingle(files[0], Path.GetDirectoryName(files[0])!, args.OptNobuild, args.OptVerbose);
		}

		private static int CompileSingle(string filename, string baseFolder, bool noBuild, bool verbose)
		{
			var output = Path.Combine(baseFolder, Path.GetFileNameWithoutExtension(filename));
			Directory.CreateDirectory(output);
			Environment.CurrentDirectory = baseFolder;
			Console.WriteLine($"{DateTime.Now}: {filename} - Compiling PanSQL script to C# project. ");
			var compiler = new Compiler.Compiler();
			var name = Path.GetFileNameWithoutExtension(filename);
			var result = compiler.Compile(name, File.ReadAllText(filename));
			Console.WriteLine($"{DateTime.Now}: PanSQL -> C# conversion successful.  Building project.");
			var buildResult = ProcessResult(name, result, output, noBuild, verbose);
			if (buildResult != 0) {
				Console.WriteLine($"{DateTime.Now}: Build failed.");
			}
			return buildResult;
		}

		private static int CompileMultiple(string[] files, bool noBuild, bool verbose)
		{
			var baseFolder = Path.Combine(Path.GetDirectoryName(files[0])!, "PanSQL_Project");
			var compiler = new Compiler.Compiler();
			Console.WriteLine($"{DateTime.Now}: Compiling PanSQL scripts to C# projects.");
			try {
				var results = compiler.CompileFiles(baseFolder, files);
				Console.WriteLine($"{DateTime.Now}: PanSQL -> C# conversion successful.  Building projects.");
				return ProcessResultSln(results, baseFolder, noBuild, verbose);
			} catch (FileNotFoundException fnf) {
				Console.WriteLine($"{DateTime.Now}: Unable to open '{fnf.FileName}.'");
				return -1;
			} catch ( CompilerError ce ) {
				Console.WriteLine($"{DateTime.Now}: {ce.Message}");
				return -1;
			}
		}

		private const string CONNECTORS_FILE = "connectors.pansync";

		private static int ProcessResult(string name, Script result, string basePath, bool noBuild, bool verbose)
		{
			if (Directory.Exists(basePath)) {
				Directory.Delete(basePath, true);
			}
			Directory.CreateDirectory(basePath);
			File.WriteAllText(Path.Combine(basePath, $"{name}.cs"), result.Code);
			var project = Path.Combine(basePath, $"{name}.csproj");
			File.WriteAllText(project, result.ProjectFile);
			File.WriteAllText(Path.Combine(basePath, CONNECTORS_FILE), result.Connectors);
			if (!noBuild) { 
				var buildFolder = Path.Combine(basePath, "Build");
				var publish = RunDotnetCommand($"publish \"{project}\" --nologo -o \"{buildFolder}\" -c Release", basePath, verbose);
				if (publish != 0 ) {
					return publish;
				}
				Console.WriteLine();
				Console.WriteLine($"{DateTime.Now}: Build successful.  Your project can be found in '{buildFolder}'.");
			}
			return 0;
		}

		private static int ProcessResultSln(Script[] projects, string basePath, bool noBuild, bool verbose)
		{
			if (projects.Length == 0) {
				Console.WriteLine($"{DateTime.Now}: No script files found.");
				return -1;
			}
			if (Directory.Exists(basePath)) {
				Directory.Delete(basePath, true);
			}
			Directory.CreateDirectory(basePath);
			var result = RunDotnetCommand("new sln", basePath, verbose);
			if (result != 0) {
				return result;
			}
			var projectFiles = new List<string>();
			foreach (var project in projects) {
				var projectFolder = Path.Combine(basePath, project.Name);
				Directory.CreateDirectory(projectFolder);
				File.WriteAllText(Path.Combine(projectFolder, $"{project.Name}.cs"), project.Code);
				var csproj = Path.Combine(projectFolder, $"{project.Name}.csproj");
				File.WriteAllText(csproj, project.ProjectFile);
				File.WriteAllText(Path.Combine(projectFolder, CONNECTORS_FILE), project.Connectors);
				result = RunDotnetCommand($"sln add \"{csproj}\"", basePath, verbose);
				if (result != 0) {
					return result;
				}
				projectFiles.Add(csproj);
			}
			if (!noBuild) {
				foreach (var project in projectFiles) {
					result = RunDotnetCommand($"publish \"{project}\" --nologo -o \"{Path.Combine(basePath, "Build", Path.GetFileNameWithoutExtension(project))}\" -c Release", basePath, verbose);
					if (result != 0) {
						return result;
					}
				}
				Console.WriteLine();
				Console.WriteLine($"{DateTime.Now}: Build successful.  All of your projects can be found under '{Path.Combine(basePath, "Build")}'.");
			}
			return 0;
		}

		private static int RunDotnetCommand(string args, string workingDir, bool verbose)
		{
			var info = new ProcessStartInfo("dotnet") { Arguments = args, WorkingDirectory = workingDir };
			if (verbose) {
				Console.WriteLine($"{DateTime.Now}: Executing command: 'dotnet {args}'.  Working directory: {workingDir}");
			}
			using var process = Process.Start(info)!;
			process.WaitForExit();
			return process.ExitCode;
		}

		static int Main(string[] args) => ProgramArguments.CreateParser()
			.WithVersion("Pansynchro PanSQL compiler, v0.1 alpha")
			.Parse(args)
			.Match(Run,
					result => ShowHelp(result.Help),
					result => ShowVersion(result.Version),
					result => OnError(result.Usage));
	}
}
