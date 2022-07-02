using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DotNet.Globbing;
using DotNet.Globbing.Token;

using Pansynchro.Core.Helpers;

namespace Pansynchro.Sources.Files
{
    public class GlobFileScanner
    {
        private readonly string _pattern;
        private readonly Glob _filter;
        private readonly bool _literal;
        private readonly IGlobToken[] _baseDir;
        private readonly bool _recursive;

        public GlobFileScanner(string pattern)
        {
            _pattern = pattern;
            _filter = Glob.Parse(pattern);
            _literal = GlobHelper.IsLiteralPattern(_filter);
            _baseDir = GlobHelper.ExtractBaseDirectory(_filter);
            _recursive = GlobHelper.LastDirSeparatorIndex(_filter) > _baseDir.Length - 1;
        }

        private string[]? _files;

        public string[] Files {
            get {
                _files ??= ScanFiles();
                return _files;
            }
        }

        private string[] ScanFiles()
        {
            if (_literal) {
                return File.Exists(_pattern) ? new string[] { _pattern } : Array.Empty<string>();
            }
            var baseDir = GlobHelper.PatternToString(_baseDir);
            return _recursive ? ScanDirRecursively(baseDir, _baseDir.Length).ToArray() : ScanDir(baseDir);
        }

        private string[] ScanDir(string path) 
            => Directory.GetFiles(path).Where(_filter.IsMatch).ToArray();

        private IEnumerable<string> ScanDirRecursively(string dir, int patternIndex)
        {
            foreach (var filename in Directory.EnumerateFiles(dir, "*.*", SearchOption.AllDirectories)) {
                if (_filter.IsMatch(filename)) {
                    yield return filename;
                }
            }
        }
    }
}
