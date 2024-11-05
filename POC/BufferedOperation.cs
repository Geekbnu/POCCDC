namespace POC
{
    public class BufferedOperation
    {
        public string TableName { get; set; }
        public GenericEnvelope Envelope { get; set; }
        public string Operation { get; set; }
        public bool IsProcessed { get; set; } = false; // Indica se a operação já foi processada
    }
}
