using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using ExcelDataReader;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Excel
{
    public class ExcelAnalyzer : ISchemaAnalyzer, ISourcedConnector
    {
        private IDataSource? _source;

        public async ValueTask<DataDictionary> AnalyzeAsync(string dictName)
        {
            if (_source == null)
            {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            var defs = new List<StreamDefinition>();
            await foreach (var (name, stream) in _source.GetDataAsync())
            {
                using var excelReader = ExcelReaderFactory.CreateReader(stream);
                do
                {
                    defs.Add(AnalyzeReader(name, excelReader));
                } while (excelReader.NextResult());
            }
            return new DataDictionary(dictName, defs.ToArray());
        }

        private const int ANALYZER_ROWS = 10;

        private static StreamDefinition AnalyzeReader(string fileName, IExcelDataReader excelReader)
        {
            var name = excelReader.CodeName;
            var fields = new List<FieldDefinition>();
            for (int i = 0; i < excelReader.FieldCount; ++i) {
                fields.Add(new FieldDefinition(excelReader.GetString(i), new FieldType(TypeTag.Unstructured, false, CollectionType.None, null)));
            }
            for (int i = 0; i < ANALYZER_ROWS; ++i) {
                if (!excelReader.Read()) {
                    break;
                }
                for (int j = 0; j < fields.Count; ++j) {
                    fields[j] = ReadTypeInfo(fields[j], excelReader.GetFieldType(j));
                }
            }
            return new StreamDefinition(new StreamDescription(fileName, name), fields.ToArray(), Array.Empty<string>());
        }

        private static FieldDefinition ReadTypeInfo(FieldDefinition fd, Type type)
        {
            if (type == null) { 
                return fd.Type.Nullable ? fd : fd with { Type = fd.Type with { Nullable = true } };
            }
            var tag = GetTypeTag(type);
            if (tag == fd.Type.Type) {
                return fd;
            }
            return fd with { Type = fd.Type with { Type = MergeTypes(tag, fd.Type.Type) } };
        }

        private static TypeTag MergeTypes(TypeTag l, TypeTag r)
        {
            if ((l == TypeTag.Int && r == TypeTag.Double) || (r == TypeTag.Int && l == TypeTag.Double)) {
                return TypeTag.Double;
            }
            return TypeTag.Text;
        }

        private static TypeTag GetTypeTag(Type type)
        {
            if (type == typeof(bool)) return TypeTag.Boolean;
            if (type == typeof(int)) return TypeTag.Int;
            if (type == typeof(double)) return TypeTag.Double;
            if (type == typeof(DateTime)) return TypeTag.DateTime;
            if (type == typeof(TimeSpan)) return TypeTag.Time;
            if (type == typeof(string)) return TypeTag.Text;
            throw new ArgumentException($"Unknown type: {type.FullName}");
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }
    }
}
