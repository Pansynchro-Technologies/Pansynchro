namespace Pansynchro.Sources.GoogleCloudStorage
{
    public record GcsPattern(string Pattern, string StreamName);

    public record GcsWriteConfig(string Bucket, GcsPattern[] Files, string? MissingFilenameSpec, int MaxParallelism = 0);

    public record GcsReadConfig(string Bucket, GcsPattern[] Streams, int MaxParallelism = 0);
}
