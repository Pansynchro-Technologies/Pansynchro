using Pansynchro.Sources.Files;

record S3Connection(string AccessKeyId, string SecretAccessKey, string RegionCode);

record S3Pattern(string Pattern, string StreamName);

record S3Key(string StreamName, string Bucket, string Filename);

record S3Config(S3Connection Conn, string Bucket, S3Pattern[] Files);

record S3WriteConfig(S3Connection Conn, string Bucket, S3Pattern[] Files, string? MissingFilenameSpec, int UploadPartSize = 5)
    : S3Config(Conn, Bucket, Files);

public class Program
{
    public static void Main()
    {
        var schema = NJsonSchema.JsonSchema.FromType<S3WriteConfig>().ToJson();
        Console.WriteLine(schema);
    }
}