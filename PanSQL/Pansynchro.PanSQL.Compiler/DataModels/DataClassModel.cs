namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal record DataClassModel(string Name, DataFieldModel[] Fields, int[] PkIndex)
	{
		public bool FieldConstructor { get; internal set; }
	}

	internal record DataFieldModel(string Name, string Type, string? Initializer, bool IsProp = false, bool IsReadonly = false, bool IsPublic = false);
}
