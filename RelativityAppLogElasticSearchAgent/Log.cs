using Nest;
using System;

namespace RelativityAppLogElasticSearchAgent
{
    /*
     * Class representing log
     */
    [ElasticsearchType(RelationName = "log")]
    public class Log
    {
        [Number(NumberType.Long, Name = "log_id", Index = true, Coerce = true, DocValues = true)]
        public long LogId { get; set; }

        [Text(Name = "message", Index = true)]
        public string Message { get; set; }

        [Keyword(Name = "level", Index = true, DocValues = true)]
        public string Level { get; set; }

        [Date(Name = "timestamp", Index = true, DocValues = true)]
        public DateTime TimeStamp { get; set; }

        [Text(Name = "exception", Index = true)]
        public string Exception { get; set; }

        [Text(Name = "properties", Index = true)]
        public string Properties { get; set; }
    }
}
