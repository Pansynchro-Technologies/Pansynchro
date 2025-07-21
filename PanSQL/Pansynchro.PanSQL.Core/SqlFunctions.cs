using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;

using Json.Path;

namespace Pansynchro.PanSQL.Core;

public static partial class SqlFunctions
{
	private static readonly double DEG_TO_RAD = Math.PI / 180.0;
	private static readonly double RAD_TO_DEG = 180.0 / Math.PI;
	private static readonly float DEG_TO_RAD_F = MathF.PI / 180.0f;
	private static readonly float RAD_TO_DEG_F = 180.0f / MathF.PI;
	private static System.Random _rng = new System.Random();

	public static int Degrees(int radians) => (int)(radians * RAD_TO_DEG);
	public static float Degrees(float radians) => radians * RAD_TO_DEG_F;
	public static double Degrees(double radians) => radians * RAD_TO_DEG;
	public static decimal Degrees(decimal radians) => (decimal)((double)radians * RAD_TO_DEG);

	public static int Radians(int degrees) => (int)(degrees * DEG_TO_RAD);
	public static float Radians(float degrees) => degrees * DEG_TO_RAD_F;
	public static double Radians(double degrees) => degrees * DEG_TO_RAD;
	public static decimal Radians(decimal degrees) => (decimal)((double)degrees * DEG_TO_RAD);

	public static double Rand(int seed)
	{
		_rng = new Random(seed);
		return _rng.NextDouble();
	}
	public static double Rand() => _rng.NextDouble();

	public static T Square<T>(T arg) where T : System.Numerics.INumber<T> => arg * arg;

	private static Dictionary<string, JsonPath> _paths = [];

	public static string? JsonValue(JsonNode value, string path)
	{
		if (!_paths.TryGetValue(path, out var pathObj)) {
			pathObj = JsonPath.Parse(path);
			_paths[path] = pathObj;
		}
		try {
			var result = pathObj.Evaluate(value);
			return result.Matches.TryGetSingleValue(out var node) ? (string?)node : null;
		} catch {
			return null;
		}
	}

	public static string StrLeft(string value, int length) => value[..length];

	public static string StrRight(string value, int length) => value[^length..];

	public static T? TryCastV<T, U>(U value) where T : struct
	{
		try {
			return (T)Convert.ChangeType(value, typeof(T))!;
		} catch {
			return null;
		}
	}

	public static T? TryCastR<T, U>(U value) where T : class
	{
		try {
			return (T)Convert.ChangeType(value, typeof(T))!;
		} catch {
			return null;
		}
	}

	public static T? TryParseV<T>(string value) where T : struct, IParsable<T>
		=> T.TryParse(value, null, out var result) ? result : null;

	public static T? TryParseR<T>(string value) where T : class, IParsable<T>
		=> T.TryParse(value, null, out var result) ? result : null;

	public static int FileCRC(string filename)
	{
		unchecked {
			return (int)System.IO.Hashing.Crc32.HashToUInt32(File.ReadAllBytes(filename));
		}
	}
}
