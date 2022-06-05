#region license
// Copyright (c) 2007, Georges Benatti Jr
// All rights reserved.
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
// Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer. 
// Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution. 
// Neither the name of Georges Benatti Jr nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission. 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#endregion

using System;
using System.IO;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.Sources.Files
{
	// adapted from FileSet class found at
	// https://github.com/boo-language/boo-build-system/blob/master/src/extensions/bake.io.extensions/FileSet.boo
	class FileSet
    {
		private class Entry {
			public Regex RegEx { get; }
			public bool IsRecursive { get; }
			public string BaseDirectory { get; }

			public Entry(Regex re, string baseDir, bool recursive)
			{
				RegEx = re;
				IsRecursive = recursive;
				BaseDirectory = baseDir;
			}
		}

		// Escape windows' path separator if neccecary
		static readonly string _separator = Path.DirectorySeparatorChar.ToString(CultureInfo.InvariantCulture).Replace("\\", "\\\\");

		// replacements to do on the incoming patterns
		// the right side is the regex, the left side the replacing string
		// order IS meaningful
		static readonly KeyValuePair<string, string>[] _patterns = new KeyValuePair<string, string>[]{
			KeyValuePair.Create("\\.","\\."),
			KeyValuePair.Create("\\$","\\$"),
			KeyValuePair.Create("\\^","\\^"),
			KeyValuePair.Create("\\{","\\{"),
			KeyValuePair.Create("\\[","\\["),
			KeyValuePair.Create("\\(","\\("),
			KeyValuePair.Create("\\)","\\)"),
			KeyValuePair.Create("\\+","\\+"),
			//Select single character which is not a separator
			KeyValuePair.Create("\\?","[^"+_separator+"]?"),
			// Replace /*/ or /* with a search for 1..n instead of 0..n
			// This makes sure that /*/ can't match "//" and that /ayende/* doesn't 
			// match "/ayende/"
			KeyValuePair.Create("(?<="+_separator+")\\*(?=($|"+_separator+"))","[^"+_separator+"]+"),
			// Handle matching in the current directory an in all subfolders, so things
			// like src/**/*.cs will work
			// the ".|" is a placeholder, to avoid overwriting the value in the next regexes
			KeyValuePair.Create(_separator + "\\*\\*" + _separator, _separator + "(.|?" + _separator + ")?"),
			KeyValuePair.Create("\\*\\*" + _separator, ".|(?<=^|" + _separator + ")"),
			KeyValuePair.Create("\\*\\*",".|"),
			KeyValuePair.Create("\\*","[^"+_separator+"]*"),
			// Here we fix all the .| problems we had before
			KeyValuePair.Create("\\.\\|","\\.*"),
			// This handles the case where the path is recursive but it doesn't ends with a
			// wild card, for example: **/bin, you want all the bin directories, but nothing more
			KeyValuePair.Create("(?<=[^\\?\\*])$","$")
		};

		public string BaseDirectory { get; set; } = Environment.CurrentDirectory;
		public bool ThrowOnEmpty { get; set; } = false;

		private List<string> _files = new();
		private List<string> _directories = new();
		private List<string> _scannedDirectories = new();
		private readonly Dictionary<string, bool> _directoriesToScan = new();
		private bool _scanned = false;

		private readonly List<Entry> _includes = new();
		private readonly List<Entry> _excludes = new();

		public FileSet()
        {
			BaseDirectory = new DirectoryInfo(BaseDirectory).FullName;
		}

		public FileSet(string spec) : this()
        {
			Include(spec);
        }

		public FileSet(IEnumerable<string> specs) : this()
        {
			foreach (var spec in specs) {
				Include(spec);
			}
        }

        public FileSet Include(string pattern)
        {
			AddRegEx(pattern, _includes);
			return this;
		}

		public FileSet Exclude(string pattern)
		{
			AddRegEx(pattern, _excludes);
			return this;
		}

		private Entry AddRegEx(string pattern, List<Entry> list)
		{
			var cleanPattern = CleanPattern(pattern);
			var firstWildCardIndex = cleanPattern.IndexOfAny("?*".ToCharArray());
			var lastOriginalDirSeparator = cleanPattern.LastIndexOf(Path.DirectorySeparatorChar);
			var modifiedPattern = firstWildCardIndex != -1 ? cleanPattern.Substring(0, firstWildCardIndex) : cleanPattern;
			var lastDirSeparatorWithoutWildCards = modifiedPattern.LastIndexOf(Path.DirectorySeparatorChar);
			var regexPattern = cleanPattern[(lastDirSeparatorWithoutWildCards + 1)..];

			var recursive = IsRecursive(cleanPattern, firstWildCardIndex, lastOriginalDirSeparator);
			var baseDir = GetPatternDirectory(modifiedPattern, lastDirSeparatorWithoutWildCards);
			var re = FormatRegex(regexPattern);
			var entry = new Entry(re, baseDir, recursive);

			// add directory to search and set it to recurse if it isn't already
			if (!_directoriesToScan.ContainsKey(baseDir)) {
				_directoriesToScan.Add(baseDir, recursive);
			}
			// This won't set it if it's false, so we won't override a
			// value of true.
			if (recursive) {
				_directoriesToScan[baseDir] = true;
			}

			list.Add(entry);
			return entry;
		}

        private string GetPatternDirectory(string pattern, int lastDirSeparator)
        {
            string searchDir;
            if (lastDirSeparator != -1) {
				searchDir = pattern.Substring(0, lastDirSeparator);
				if (searchDir.EndsWith(Path.VolumeSeparatorChar.ToString())) {
					searchDir += Path.DirectorySeparatorChar;
				}
			} else {
				searchDir = string.Empty;
			}

			if (!Path.IsPathRooted(searchDir)) {
				searchDir = Path.Combine(BaseDirectory, searchDir);
			}

			return new DirectoryInfo(searchDir).FullName;
		}

		// A pattern is recursive if:
		// 	- It contain ** - the directory wildcard
		//	- It contain a wildcard before the last directory seperator
		//	- It ends with a directory seperator - but this is handled in the CleanPattern() method
		private static bool IsRecursive(string pattern, int firstWildCardIndex, int lastDirSeperator)
		{
			var hasDirWildCard = pattern.IndexOf("**") != -1;
			return hasDirWildCard || firstWildCardIndex < lastDirSeperator;
		}

		public List<string> Files
        {
			get {
				if (!_scanned) {
					Scan();
                }
				return _files;
            }
        }

		public List<string> Directories
		{
			get {
				if (!_scanned) {
					Scan();
				}
				return _directories;
			}
		}

		private static Regex FormatRegex(string pattern)
		{
			var result = pattern;
			foreach (var (re, replacement) in _patterns) {
				result = new Regex(re).Replace(result, replacement);
			}
			var reOpts = RegexOptions.Compiled;
			if (!CaseSensitiveFileSystem()) {
				reOpts |= RegexOptions.IgnoreCase;
			}
			return new Regex(result, reOpts);
		}

		public bool ContainFileMoreRecentThan(DateTime lastWrite) 
			=> Files.Any(f => File.GetLastWriteTime(f) > lastWrite);

		// replace '/' and '\' with the platform directory seperator
		// apped ** if the path ends with a directory seperator
		private static string CleanPattern(string pattern)
		{
			var s = DoCleanPattern(pattern);
			if (s.EndsWith(Path.DirectorySeparatorChar.ToString())) {
				s += "**";
			}
			return s;
		}

		public virtual void Scan()
		{
			_files = new List<string>();
			_directories = new List<string>();
			_scannedDirectories = new List<string>();
			_scanned = false;
			foreach (var entry in _directoriesToScan) {
				ScanDir(entry.Key, entry.Value);
			}
			_scanned = true;
			if (ThrowOnEmpty && _directories.Count == 0 && _files.Count == 0) {
				throw new FileSetEmptyException();
			}
		}

		protected void ScanDir(string path, bool recursive)
		{
			if (_scannedDirectories.Contains(path)) {
				return;
			}
			_scannedDirectories.Add(path);
			if (!Directory.Exists(path)) {
				return;
			}

			var pathCompare = CaseSensitiveFileSystem() ? path : path.ToLower();

			var includeEntries = GetEntriesForPath(pathCompare, _includes);
			var excludeEntries = GetEntriesForPath(pathCompare, _excludes);

			foreach (var directory in Directory.GetDirectories(path)) {
				// If recursive, then it'll have a chance to add itself on the recursed ScanDir
				// otherwise, let it have a go and add it if need to
				if (recursive) {
					ScanDir(directory, recursive);
				} else if (IncludePath(directory, includeEntries, excludeEntries)) {
					_directories.Add(directory);
				}
			}

			foreach (var file in Directory.GetFiles(path)) {
				if (IncludePath(file, includeEntries, excludeEntries)) {
					_files.Add(file);
				}
			}

			if (IncludePath(pathCompare, includeEntries, excludeEntries)) {
				_directories.Add(path);
			}
		}

		private static bool IncludePath(string path, List<Entry> includeEntries, List<Entry> excludeEntries)
			=> includeEntries.Any(e => CheckPath(e, path)) && excludeEntries.All(e => !CheckPath(e, path));

		private static bool CheckPath(Entry entry, string path)
		{
			var length = entry.BaseDirectory.Length;
			var pathWithoutBase = path[length..];
			return entry.RegEx.IsMatch(pathWithoutBase);
		}

		private static List<Entry> GetEntriesForPath(string path, List<Entry> list)
		{
			var entries = new List<Entry>();
			foreach (var entry in list) {
				var baseDir = entry.BaseDirectory;
				if (!CaseSensitiveFileSystem()) {
					baseDir = baseDir.ToLower();
				}
				if (AreEqual(path, baseDir)) {
					entries.Add(entry);
				} else {
					if (entry.IsRecursive) {
						if (!baseDir.EndsWith(Path.DirectorySeparatorChar.ToString())) {
							baseDir += Path.DirectorySeparatorChar;
						}
						if (path.StartsWith(baseDir)) {
							entries.Add(entry);
						}
					}
				}
			}
			return entries;
		}

		private static bool AreEqual(string first, string second)
		{
			var co = CompareOptions.None;
			if (!CaseSensitiveFileSystem()) {
				co |= CompareOptions.IgnoreCase;
			}
			return CultureInfo.InvariantCulture.CompareInfo.Compare(first, second, co) == 0;
		}

		// Assume that only unix is case sensitive because Win32 & Mac OS are not
		private static bool CaseSensitiveFileSystem()
			=> Environment.OSVersion.Platform == PlatformID.Unix;

		// Convert / or \ to the normal directory seperator
		private static string DoCleanPattern(string pattern)
		{
			var s = pattern.Replace('/', Path.DirectorySeparatorChar);
			return s.Replace('\\', Path.DirectorySeparatorChar);
		}
	}

	class FileSetEmptyException : Exception
	{
		public FileSetEmptyException() : base() { }
	}
}
