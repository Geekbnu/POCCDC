namespace POC.Domain
{
    public class KeyPar
    {
        public List<Key> Keys { get; set; } = new();
        public string Table { get; set; }
        public string Schema { get; set; }
        public string Connector { get; set; }
    }

    public class Key
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
