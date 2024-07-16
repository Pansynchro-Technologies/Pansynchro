using System.IO;
using System.Runtime.CompilerServices;

using Microsoft.SqlServer.Types;

using Pansynchro.Core.CustomTypes;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.MSSQL
{
	internal static class MssqlCustomTypes
	{
		private class HierarchySupport : ICustomType
		{
			public string Name => typeof(SqlHierarchyId).FullName!;

			public TypeTag Type => TypeTag.HierarchyID;

			public object ProtocolReader(BinaryReader r)
			{
				// this is horrifically inefficient, but SqlHierarchyId's broken serialization requires this as a workaround
				// https://github.com/dotMorten/Microsoft.SqlServer.Types/issues/64
				var result = new SqlHierarchyId();
				var len = r.Read7BitEncodedInt();
				using var ms = new MemoryStream(r.ReadBytes(len));
				using var br = new BinaryReader(ms);
				result.Read(br);
				return result;
			}

			public void ProtocolWriter(object o, BinaryWriter s)
			{
				// this is horrifically inefficient, but SqlHierarchyId's broken serialization requires this as a workaround
				// https://github.com/dotMorten/Microsoft.SqlServer.Types/issues/64
				var id = (SqlHierarchyId)o;
				using var ms = new MemoryStream();
				using var sw = new BinaryWriter(ms);
				id.Write(sw);
				s.Write7BitEncodedInt((int)ms.Length);
				s.Write(ms.GetBuffer(), 0, (int)ms.Length);
			}
		}

		private class GeometrySupport : ICustomType
		{
			public string Name => typeof(SqlGeometry).FullName!;

			public TypeTag Type => TypeTag.Geometry;

			public object ProtocolReader(BinaryReader r)
			{
				var result = new SqlGeometry();
				result.Read(r);
				return result;
			}

			public void ProtocolWriter(object o, BinaryWriter s)
			{
				var value = (SqlGeometry)o;
				value.Write(s);
			}
		}

		private class GeographySupport : ICustomType
		{
			public string Name => typeof(SqlGeography).FullName!;

			public TypeTag Type => TypeTag.Geometry;

			public object ProtocolReader(BinaryReader r)
			{
				var result = new SqlGeography();
				result.Read(r);
				return result;
			}

			public void ProtocolWriter(object o, BinaryWriter s)
			{
				var value = (SqlGeography)o;
				value.Write(s);
			}
		}

		[ModuleInitializer]
		public static void Register()
		{
			CustomTypeRegistry.RegisterType(new HierarchySupport());
			CustomTypeRegistry.RegisterType(new GeometrySupport());
		}
	}
}
