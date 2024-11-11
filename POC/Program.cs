using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using POC.Domain;
using POC.Interfaces;
using POC.Services;

namespace ForeignKeysQuery
{
    class Program
    {
        private static IServiceProvider _serviceProvider;

        static async Task Main(string[] args)
        {
            ServiceBind();

            var _redisCache = _serviceProvider.GetService<IRedisCacheService>();
            var _property = _serviceProvider.GetService<IPropertyService>();
            var _produce = _serviceProvider.GetService<IKafkaProducerService>();
            var _foreignKeyService = _serviceProvider.GetService<IForeignKeyService>();
            var _databaseService = _serviceProvider.GetService<IDatabaseService>();
            var _consumer = _serviceProvider.GetService<IConsumer<Ignore, string>>();

            var kafkaConsumer = new KafkaConsumerService(_redisCache, _property, _produce, _foreignKeyService, _databaseService, _consumer);

            await kafkaConsumer.ConsumeMessages();
        }

        public static void ServiceBind()
        {
            var config = new ConsumerConfig
            {
            };

            _serviceProvider = new ServiceCollection()
                .AddSingleton<IRedisCacheService, RedisCacheService>()
                .AddSingleton<IDatabaseService, DatabaseServiceSQLServerToPostgres>()
                .AddSingleton<IKafkaProducerService, KafkaProducerService>()
                .AddSingleton<IForeignKeyService, ForeignKeyService>()
                .AddSingleton<IPropertyService, PropertyService>()
                .AddSingleton<IConsumer<Ignore, string>>(sp =>
                {
                    var propertyService = sp.GetRequiredService<IPropertyService>();
                    var config = new ConsumerConfig
                    {
                        GroupId = propertyService.GetProperty(Property.GROUPID),
                        BootstrapServers = propertyService.GetProperty(Property.BOOTSTRAP),
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        EnableAutoCommit = false
                    };

                    return new ConsumerBuilder<Ignore, string>(config).Build();
                })
                .BuildServiceProvider();
        }
    }
}
