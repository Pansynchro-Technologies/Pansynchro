using System.Collections.Generic;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.CustomTypes
{
	public static class CustomTypeRegistry
	{
		private static Dictionary<string, ICustomType> _customRegistry = new();
		private static Dictionary<TypeTag, ICustomType> _registry = new();

		public static void RegisterType(ICustomType type)
		{
			var tag = type.Type;
			if (tag == TypeTag.Custom) {
				_customRegistry.Add(type.Name, type);
			} else {
				_registry.Add(tag, type);
			}
		}

		public static ICustomType? GetType(TypeTag type)
		{
			_registry.TryGetValue(type, out var result);
			return result;
		}
	}
}
