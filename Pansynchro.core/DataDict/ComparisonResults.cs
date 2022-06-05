namespace Pansynchro.Core.DataDict
{
    public abstract record ComparisonResult(string Message);

    public record ComparisonError(string Message): ComparisonResult(Message);

    public abstract record ConversionLine(string Field) : ComparisonResult("");

    public record PromotionLine(string Field, TypeTag NewType): ConversionLine(Field);

    public record NamedConversionLine(string Field, string ConversionName): ConversionLine(Field);
}
