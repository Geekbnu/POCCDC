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

            var kafkaConsumer = new KafkaConsumerService(_redisCache, _property, _produce, _foreignKeyService, _databaseService);
            await kafkaConsumer.ConsumeMessages();
        }

        public static void ServiceBind()
        {
            _serviceProvider = new ServiceCollection()
                .AddSingleton<IRedisCacheService, RedisCacheService>()
                .AddSingleton<IDatabaseService, DatabaseServiceSQLServerToPostgres>()
                .AddSingleton<IKafkaProducerService, KafkaProducerService>()
                .AddSingleton<IForeignKeyService, ForeignKeyService>()
                .AddSingleton<IPropertyService, PropertyService>()
                .BuildServiceProvider();
        }
    }
}
