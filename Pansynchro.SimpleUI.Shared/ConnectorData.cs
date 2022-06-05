namespace Pansynchro.SimpleUI.Shared
{
    public record ConnectorData(
        string Name,
        string Description,
        bool Reader,
        bool Writer,
        ConfigElement[] Config
    );

    public record ConfigElement (
        string Name,
        string Description,
        ConfigType Type,
        object? Default,
        string[]? Options
    );

    public enum ConfigType
    {
        Text,
        Integer,
        Boolean,
        Picklist
    }
}
