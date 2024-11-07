using Confluent.Kafka;
using POC.Interfaces;

namespace POC.Services
{
    public class KafkaProducerService : IKafkaProducerService
    {
        private readonly IProducer<Null, string> _producer;

        public KafkaProducerService()
        {
            var configProducer = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };

            _producer = new ProducerBuilder<Null, string>(configProducer).Build();
        }

        public async Task ProduceRetryMessage(string topic, string message)
        {
            var retryMessage = new Message<Null, string> { Value = message };
            _producer.Produce(topic, retryMessage, (deliveryReport) =>
            {
                if (deliveryReport.Error.Code != ErrorCode.NoError)
                {
                    Console.WriteLine($"Error sending retry message: {deliveryReport.Error.Reason}");
                }
                else
                {
                    Console.WriteLine("Retry message sent to end of queue.");
                }
            });
        }
    }
}
