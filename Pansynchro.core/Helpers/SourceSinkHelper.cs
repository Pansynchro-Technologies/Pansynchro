namespace Pansynchro.Core.Helpers
{
    public static class SourceSinkHelper
    {
        public static IDataSource Pipeline(this IDataSource source, params IDataInputProcessor[] procs)
        {
            var result = source;
            foreach (var processor in procs) {
                processor.SetDataSource(result);
                result = processor;
            }
            return result;
        }

        public static IDataSink Pipeline(this IDataSink sink, params IDataOutputProcessor[] procs)
        {
            var result = sink;
            foreach (var processor in procs) {
                processor.SetDataSink(result);
                result = processor;
            }
            return result;
        }
    }
}
