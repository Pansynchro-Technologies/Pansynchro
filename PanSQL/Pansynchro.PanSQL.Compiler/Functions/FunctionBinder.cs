using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using Pansynchro.PanSQL.Compiler.Steps;

namespace Pansynchro.PanSQL.Compiler.Functions
{
	internal static class FunctionBinder
	{
		internal static Dictionary<string, MethodInfo> _methods = new(StringComparer.InvariantCultureIgnoreCase);
		internal static Dictionary<string, PropertyInfo> _props = new(StringComparer.InvariantCultureIgnoreCase);

		static FunctionBinder()
		{
			_methods.Add("format", typeof(string).GetMethod("Format", [typeof(string), typeof(object[])])!);
			_props.Add("current_timestamp", typeof(DateTime).GetProperty("Now")!);
			_props.Add("getdate", _props["current_timestamp"]);
			_props.Add("getutcdate", typeof(DateTime).GetProperty("UtcNow")!);
			_props.Add("pi", typeof(Math).GetProperty("PI")!);
		}

		internal static void Bind(FunctionCallExpression node)
		{
			if (!_methods.TryGetValue(node.Method, out var info)) {
				throw new CompilerError($"No function named '{node.Method}' is available", node);
			}
			var parameters = info.GetParameters();
			ParameterInfo? paramsArg = null;
			if (parameters.Length > 0 && Attribute.IsDefined(parameters[^1], typeof(ParamArrayAttribute))) {
				paramsArg = parameters[^1];
				parameters = parameters[..^1];
			}
			if ((paramsArg == null && node.Args.Length != parameters.Length)
			 || (paramsArg != null && node.Args.Length < parameters.Length)){
				throw new CompilerError($"Function '{node.Method}' requires {(paramsArg != null ? "at least " : "")}{parameters.Length} arguments.", node);
			}
			TypeCheck(node.Args, parameters, paramsArg);
			node.CodeName = $"{info.DeclaringType!.Name}.{info.Name}";
			node.Namespace = info.DeclaringType.Namespace!;
			node.ReturnType = TypesHelper.CSharpTypeToFieldType(info.ReturnType);
		}

		internal static void Bind(CallExpression call, Node parent)
		{
			if (!_methods.TryGetValue(call.Function.Name, out var info)) {
				if (_props.TryGetValue(call.Function.Name, out var prop)) {
					BindProp(call, prop, parent);
					return;
				}
				throw new CompilerError($"No function named '{call.Function}' is available", parent);
			}
			var parameters = info.GetParameters();
			ParameterInfo? paramsArg = null;
			if (parameters.Length > 0 && Attribute.IsDefined(parameters[^1], typeof(ParamArrayAttribute))) {
				paramsArg = parameters[^1];
				parameters = parameters[..^1];
			}
			if ((paramsArg == null && call.Args.Length != parameters.Length)
			 || (paramsArg != null && call.Args.Length < parameters.Length)){
				throw new CompilerError($"Function '{call.Function}' requires {(paramsArg != null ? "at least " : "")}{parameters.Length} arguments.", parent);
			}
			try {
				TypeCheck(call.Args, parameters, paramsArg);
			} catch (Exception e) {
				throw new CompilerError(e.Message, e, parent);
			}
			call.Function = new($"{info.DeclaringType!.Name}.{info.Name}");
			call.Type = TypesHelper.CSharpTypeToFieldType(info.ReturnType);
		}

		private static void BindProp(CallExpression call, PropertyInfo prop, Node parent)
		{
			if (call.Args.Length > 0) {
				throw new CompilerError($"Function '{call.Function}' does not take any arguments", parent);
			}
			call.IsProp = true;
			call.Function = new ReferenceExpression($"{prop.DeclaringType!.Name}.{prop.Name}");
			call.Type = TypesHelper.CSharpTypeToFieldType(prop.PropertyType);
		}

		private static void TypeCheck(Expression[] args, ParameterInfo[] parameters, ParameterInfo? paramsArg)
		{
			int i = 0;
			while (i < parameters.Length) {
				var expectedType = TypesHelper.CSharpTypeToFieldType(parameters[i].ParameterType);
				var argType = (args[i] as TypedExpression)?.ExpressionType!;
				TypeCheck(args[i], parameters[i], expectedType, argType);
				++i;
			}
			if (paramsArg != null) {
				var expectedType = TypesHelper.CSharpTypeToFieldType(paramsArg.ParameterType.GetElementType()!);
				while (i < args.Length) {
					var argType = (args[i] as TypedExpression)?.ExpressionType!;
					TypeCheck(args[i], paramsArg, expectedType, argType);
					++i;
				}
			}
		}

		private static void TypeCheck(DbExpression[] args, ParameterInfo[] parameters, ParameterInfo? paramsArg)
		{
			int i = 0;
			while (i < parameters.Length) {
				var expectedType = TypesHelper.CSharpTypeToFieldType(parameters[i].ParameterType);
				var argType = args[i].Type;
				TypeCheck(args[i], parameters[i], expectedType, argType);
				++i;
			}
			if (paramsArg != null) {
				var expectedType = TypesHelper.CSharpTypeToFieldType(paramsArg.ParameterType.GetElementType()!);
				while (i < args.Length) {
					var argType = args[i].Type;
					TypeCheck(args[i], paramsArg, expectedType, argType);
					++i;
				}
			}
		}

		private static void TypeCheck(Expression arg, ParameterInfo parameter, FieldType expectedType, FieldType? argType)
		{
			if (argType == null) {
				throw new CompilerError($"'{arg}' is not a valid function argument.", arg);
			}
			if (expectedType.Type == TypeTag.Unstructured) {
				return;
			}
			if (DataDictionaryComparer.TypeCheckField(argType, expectedType, parameter.Name!, false) != null) {
				throw new CompilerError($"'{arg}' cannot be passed to a function parameter of type '{expectedType}'.", arg);
			}
		}

		private static void TypeCheck(DbExpression arg, ParameterInfo parameter, FieldType expectedType, FieldType? argType)
		{
			if (argType == null) {
				throw new Exception($"'{arg}' is not a valid function argument.");
			}
			if (expectedType.Type == TypeTag.Unstructured) {
				return;
			}
			if (DataDictionaryComparer.TypeCheckField(argType, expectedType, parameter.Name!, false) != null) {
				throw new Exception($"'{arg}' cannot be passed to a function parameter of type '{expectedType}'.");
			}
		}
	}
}
