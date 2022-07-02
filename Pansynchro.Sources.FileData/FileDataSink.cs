using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using DotNet.Globbing;
using Newtonsoft.Json;

using Pansynchro.Core;

namespace Pansynchro.Sources.Files
{
    record SaveFileSpec(string StreamName, string Filename);
    enum DupeFilenameAction
    {
        Append,
        Overwrite,
        SequenceNumber,
        Error
    }
    record SaveFileConfig(SaveFileSpec[] Files, string MissingFilenameSpec, DupeFilenameAction DuplicateFilenameAction);

    public class FileDataSink : IDataSink
    {
        private readonly SaveFileConfig _config;
        private readonly KeyValuePair<Glob, string>[] _files;

        public FileDataSink(string config)
        {
            _config = JsonConvert.DeserializeObject<SaveFileConfig>(config) 
                ?? throw new ArgumentException("FileDataSink config is invalid");
            _files = _config.Files
                .Select(s => KeyValuePair.Create(Glob.Parse(s.StreamName), s.Filename))
                .ToArray();
        }

        public async Task<Stream> WriteData(string streamName)
        {
            var filename = GetFilename(streamName);
            if (File.Exists(filename)) {
                return _config.DuplicateFilenameAction switch {
                    DupeFilenameAction.Append => new FileStream(filename, FileMode.Append, FileAccess.Write),
                    DupeFilenameAction.Overwrite => File.Create(filename),
                    DupeFilenameAction.SequenceNumber => File.Create(FileDataSink.GetSequenceFilename(streamName)),
                    DupeFilenameAction.Error => throw new DataException($"Duplicate filename '{filename}' for stream '{streamName}' is not alowed by configuration."),
                    _ => throw new ArgumentException($"Unknown duplicate filename action: {_config.DuplicateFilenameAction}")
                };
            }
            await ValueTask.CompletedTask; //just to shut the compiler up
            return File.Create(filename);
        }

        private static string GetSequenceFilename(string streamName)
        {
            int i = 0;
            var dir = Path.GetDirectoryName(streamName)!;
            var baseFilename = Path.GetFileNameWithoutExtension(streamName);
            var ext = Path.GetExtension(streamName);
            string filename;
            do {
                ++i;
                filename = $"{baseFilename}({i}){ext}";
            } while (File.Exists(Path.Combine(dir, filename)));
            return Path.Combine(dir, filename);
        }

        private string GetFilename(string streamName)
        {
            var pair = _files.FirstOrDefault(s => s.Key.IsMatch(streamName));
            var match = pair.Value ?? _config.MissingFilenameSpec;
            if (string.IsNullOrWhiteSpace(match)) {
                throw new ArgumentException($"No filename could be generated for stream name '{streamName}'");
            }
            return match.Replace("*", streamName);
        }
    }
}
