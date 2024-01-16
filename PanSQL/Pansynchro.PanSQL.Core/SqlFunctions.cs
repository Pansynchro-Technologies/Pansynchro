using System;

namespace Pansynchro.PanSQL.Core
{
	public static class SqlFunctions
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

		public static T Square<T>(T arg) where T: System.Numerics.INumber<T> => arg * arg;
	}
}
