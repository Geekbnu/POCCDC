using Confluent.Kafka;
using Newtonsoft.Json;
using POC.Domain;
using POC.Enum;
using POC.Interfaces;
using System.Data.SqlClient;
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

        public KafkaConsumerService(IRedisCacheService redisCache, IPropertyService propertyService, IKafkaProducerService produceService,
            IForeignKeyService foreignKeyService, IDatabaseService databaseService)
        {
            _redisCache = redisCache;
            _propertyService = propertyService;
            _producerService = produceService;
            _databaseService = databaseService;
            _foreignKeyService = foreignKeyService;

            var config = new ConsumerConfig
            {
                GroupId = _propertyService.GetProperty(Property.GROUPID),
                BootstrapServers = _propertyService.GetProperty(Property.BOOTSTRAP),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        public async Task ConsumeMessages()
        {
            _consumer.Subscribe(_propertyService.GetListProperty(Property.TOPICS));

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            while (true)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cts.Token);
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

        private async Task ProcessMessage(ConsumeResult<Ignore, string> consumeResult)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();

            try
            {
                var data = JsonConvert.DeserializeObject<dynamic>(consumeResult.Message.Value);
                if (data == null) return;

                string table = data["source"]["table"];

                var op = _databaseService.GetDatabaseOperation((char)data["op"]);

                var foreignKeys = await _databaseService.GetReferencesTables();

                if (op == DatabaseOperation.UPDATE || op == DatabaseOperation.CREATE)
                {
                    var detail = data["after"];
                    var isDependencySatisfied = await _foreignKeyService.DependencyEngineSync(table, detail, foreignKeys);
                    if (isDependencySatisfied)
                    {
                        await _databaseService.ExecuteOperationAsync(table, detail, op);
                    }
                    else
                    {
                        await _producerService.ProduceRetryMessage(consumeResult.Topic, consumeResult.Message.Value);
                    }
                }
                else if (op == DatabaseOperation.DELETE)
                {
                    var detail = data["before"];
                    await _databaseService.ExecuteOperationAsync(table, detail, op);
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                await _producerService.ProduceRetryMessage(consumeResult.Topic, consumeResult.Message.Value);
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
