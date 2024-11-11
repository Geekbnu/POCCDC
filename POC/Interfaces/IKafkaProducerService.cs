using Confluent.Kafka;

namespace POC.Interfaces
{
    public interface IKafkaProducerService
    {
        Task ProduceRetryMessage(TopicPartition topic, string message);
    }
}
