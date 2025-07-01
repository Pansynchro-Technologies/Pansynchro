using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using Pansynchro.PanSQL.Core;

namespace Pansynchro.PanSQL.Compiler.Functions
{
	internal static class FunctionBinder
	{
		internal static Dictionary<string, MethodInfo> _methods = new(StringComparer.InvariantCultureIgnoreCase);
		internal static Dictionary<string, PropertyInfo> _props = new(StringComparer.InvariantCultureIgnoreCase);

		static FunctionBinder()
		{
			_methods.Add("format", typeof(string).GetMethod("Format", [typeof(string), typeof(object[])])!);
			_methods.Add("FormatDate", typeof(DateTime).GetMethod("ToString", [typeof(string)])!); 
			_methods.Add("HttpQuery", typeof(SqlFunctions).GetMethod("HttpQuery")!);
			_methods.Add("HttpQueryJson", typeof(SqlFunctions).GetMethod("HttpQueryJson")!);
			_methods.Add("HttpPost", typeof(SqlFunctions).GetMethod("HttpPost")!);
			_methods.Add("HttpPostJson", typeof(SqlFunctions).GetMethod("HttpPostJson")!);
			_methods.Add("JSON_VALUE", typeof(SqlFunctions).GetMethod("JsonValue")!);
			_methods.Add("LEFT", typeof(SqlFunctions).GetMethod("StrLeft")!);
			_methods.Add("RIGHT", typeof(SqlFunctions).GetMethod("StrRight")!);
			_methods.Add("ToUtc", typeof(DateTime).GetMethod("ToUniversalTime")!);
			_props.Add("CurrentTimestamp", typeof(DateTime).GetProperty("Now")!);
			_props.Add("getdate", _props["CurrentTimestamp"]);
			_props.Add("getutcdate", typeof(DateTime).GetProperty("UtcNow")!);
			_props.Add("pi", typeof(Math).GetProperty("PI")!);
			_props.Add("len", typeof(string).GetProperty("Length")!);
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
				var func = SpecialFunctions.ListSpecialFunctions()
					.FirstOrDefault(f =>
						f.Name.Equals(call.Function.Name, StringComparison.InvariantCultureIgnoreCase));
				if (func == default) {
					throw new CompilerError($"No function named '{call.Function}' is available", parent);
				}
				BindSpecialFunction(call, func, parent);
				return;
			}
			var parameters = info.GetParameters();
			ParameterInfo? paramsArg = null;
			if (parameters.Length > 0 && Attribute.IsDefined(parameters[^1], typeof(ParamArrayAttribute))) {
				paramsArg = parameters[^1];
				parameters = parameters[..^1];
			}
			var selfArgCount = info.IsStatic ? 0 : 1;
			if ((paramsArg == null && call.Args.Length != parameters.Length + selfArgCount)
			 || (paramsArg != null && call.Args.Length < parameters.Length + selfArgCount)) {
				throw new CompilerError($"Function '{call.Function}' requires {(paramsArg != null ? "at least " : "")}{parameters.Length} arguments.", parent);
			}
			try {
				if (!info.IsStatic) {
					TypeCheck(call.Args[0], info.DeclaringType);
				}
				TypeCheck(info.IsStatic ? call.Args : call.Args[1..], parameters, paramsArg);
			} catch (Exception e) {
				throw new CompilerError(e.Message, e, parent);
			}
			call.Function = info.IsStatic ? new($"{info.DeclaringType!.Name}.{info.Name}") : new(info.Name);
			call.Type = TypesHelper.CSharpTypeToFieldType(info.ReturnType);
			call.IsStaticMethod = info.IsStatic;
		}

		private static void BindSpecialFunction(CallExpression call, in SpecialFunc func, Node parent)
		{
			var argTypes = call.Args.Select(a => a.Type!).ToArray();
			var typeError = func.VerifyArgs(argTypes);
			if (typeError != null) {
				throw new CompilerError(string.Format(typeError, func.Name), parent);
			}
			call.Type = func.ReturnType(argTypes);
			call.SpecialCodegen = func.Codegen;
		}

		private static void BindProp(CallExpression call, PropertyInfo prop, Node parent)
		{
			var isStatic = prop.GetMethod!.IsStatic;
			if (isStatic) {
				if (call.Args.Length > 0) {
					throw new CompilerError($"Function '{call.Function}' does not take any arguments", parent);
				}
				call.IsStaticProp = true;
				call.Function = new ReferenceExpression($"{prop.DeclaringType!.Name}.{prop.Name}");
			} else {
				if (call.Args.Length != 1) {
					throw new CompilerError($"Function '{call.Function}' takes one argument", parent);
				}
				call.IsProp = true;
				call.Function = new ReferenceExpression(prop.Name);
			}
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

		private static void TypeCheck(DbExpression expr, Type typ)
		{
			var expectedType = TypesHelper.CSharpTypeToFieldType(typ);
			TypeCheck(expr, null!, expectedType, expr.Type);
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

		private static void TypeCheck(Expression arg, ParameterInfo parameter, IFieldType expectedType, IFieldType? argType)
		{
			if (argType == null) {
				throw new CompilerError($"'{arg}' is not a valid function argument.", arg);
			}
			if (expectedType is BasicField { Type: TypeTag.Unstructured }) {
				return;
			}
			if (DataDictionaryComparer.TypeCheckField(argType, expectedType, parameter.Name!, false) != null) {
				throw new CompilerError($"'{arg}' cannot be passed to a function parameter of type '{expectedType}'.", arg);
			}
		}

		private static void TypeCheck(DbExpression arg, ParameterInfo? parameter, IFieldType expectedType, IFieldType? argType)
		{
			if (argType == null) {
				throw new Exception($"'{arg}' is not a valid function argument.");
			}
			if (expectedType is BasicField { Type: TypeTag.Unstructured }) {
				return;
			}
			if (DataDictionaryComparer.TypeCheckField(argType, expectedType, parameter?.Name ?? "this", false) != null) {
				throw new Exception($"'{arg}' cannot be passed to a function parameter of type '{expectedType}'.");
			}
		}

		internal static IFieldType GetCallType(CallExpression call)
		{
			if (!_methods.TryGetValue(call.Function.Name, out var info)) {
				if (_props.TryGetValue(call.Function.Name, out var prop)) {
					return TypesHelper.CSharpTypeToFieldType(prop.PropertyType);
				}
				var func = SpecialFunctions.ListSpecialFunctions()
					.FirstOrDefault(f =>
						f.Name.Equals(call.Function.Name, StringComparison.InvariantCultureIgnoreCase));
				if (func == default) {
					throw new CompilerError($"No function named '{call.Function}' is available", null!);
				}
				return TypesHelper.NullType;
			}
			return TypesHelper.CSharpTypeToFieldType(info.ReturnType);
		}
	}
}
