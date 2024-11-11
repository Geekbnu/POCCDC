using Confluent.Kafka;
using Newtonsoft.Json;
using POC.Domain;
using POC.Enum;
using POC.Interfaces;
using System.Diagnostics;

namespace POC.Services
{
    public class KafkaConsumerService
    {
        private readonly IRedisCacheService _redisCache;
        private readonly IDatabaseService _databaseService;
        private readonly IKafkaProducerService _producerService;
        private readonly IForeignKeyService _foreignKeyService;
        private readonly IPropertyService _propertyService;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly CancellationTokenSource _cts;

        public KafkaConsumerService(IRedisCacheService redisCache, IPropertyService propertyService, IKafkaProducerService produceService,
           IForeignKeyService foreignKeyService, IDatabaseService databaseService, IConsumer<Ignore, string> consumer, CancellationTokenSource cts)
            : this(redisCache, propertyService, produceService, foreignKeyService, databaseService, consumer)
        {
            _cts = cts;
        }

        public KafkaConsumerService(IRedisCacheService redisCache, IPropertyService propertyService, IKafkaProducerService produceService,
            IForeignKeyService foreignKeyService, IDatabaseService databaseService, IConsumer<Ignore, string> consumer)
        {
            _redisCache = redisCache;
            _propertyService = propertyService;
            _producerService = produceService;
            _databaseService = databaseService;
            _foreignKeyService = foreignKeyService;
            _consumer = consumer;

            _cts = new CancellationTokenSource();
        }

        public async Task ConsumeMessages()
        {
            _consumer.Subscribe(_propertyService.GetListProperty(Property.TOPICS));

            while (true)
            {
                try
                {
                    var consumeResult = _consumer.Consume(_cts.Token);

                    if (consumeResult?.Message?.Value != null)
                    {
                        await ProcessMessage(consumeResult);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Operation canceled by user.");
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message}");
                }
            }
        }

        internal async Task ProcessMessage(ConsumeResult<Ignore, string> consumeResult)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();

            try
            {
                var data = JsonConvert.DeserializeObject<dynamic>(consumeResult.Message.Value);
                if (data == null) return;

                string table = data["source"]["table"];

                var op = _databaseService.GetDatabaseOperation((char)data["op"]);

                if (op == DatabaseOperation.UPDATE || op == DatabaseOperation.CREATE)
                {
                    var detail = data["after"];

                    var isDependencySatisfied = await _foreignKeyService.DependencyEngineSync(table, detail);

                    if (isDependencySatisfied)
                    {
                        await _databaseService.ExecuteOperationAsync(table, detail, op);
                    }
                    else
                    {
                        await _producerService.ProduceRetryMessage(consumeResult.TopicPartition, consumeResult.Message.Value);
                    }
                }
                else if (op == DatabaseOperation.DELETE)
                {
                    var detail = data["before"];
                    await _databaseService.ExecuteOperationAsync(table, detail, op);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
            }
            finally
            {
                _consumer.Commit();
                stopWatch.Stop();
                Console.WriteLine(@$"RunTime {stopWatch.Elapsed:hh\:mm\:ss\.fff}");
            }
        }
    }
}
