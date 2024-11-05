namespace POC
{
    public class GenericEnvelope
    {
        public Dictionary<string, object> Before { get; set; }
        public Dictionary<string, object> After { get; set; }
        public Dictionary<string, object> Source { get; set; }
        public string Op { get; set; }
        public long? TsMs { get; set; }
        public Dictionary<string, object> Transaction { get; set; }
    }
}
