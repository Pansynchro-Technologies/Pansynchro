namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal record DataClassModel(string Name, DataFieldModel[] Fields, int[] PkIndex);

	internal record DataFieldModel(string Name, string Type, string? Initializer, bool IsProp = false);
}
