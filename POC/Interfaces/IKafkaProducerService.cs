namespace POC.Interfaces
{
    public interface IKafkaProducerService
    {
        Task ProduceRetryMessage(string topic, string message);
    }
}
