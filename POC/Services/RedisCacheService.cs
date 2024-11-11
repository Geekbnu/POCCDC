using Newtonsoft.Json;
using POC.Domain;
using POC.Interfaces;
using StackExchange.Redis;

namespace POC.Services
{
    public class RedisCacheService : IRedisCacheService
    {
        private readonly IPropertyService _propertyService;
        private readonly ConnectionMultiplexer _redis;
        private readonly IDatabase _database;

        public RedisCacheService(IPropertyService propertyService)
        {
            _propertyService = propertyService;
            _redis = ConnectionMultiplexer.Connect(_propertyService.GetProperty(Property.CONNECTIONSTRINGREDIS));
            _database = _redis.GetDatabase();
        }

        public async Task<bool> ExistsAsync(string key)
        {
            return await _database.KeyExistsAsync(key);
        }

        public async Task SetAsync(string key, object value)
        {
            await _database.StringSetAsync(key, JsonConvert.SerializeObject(value));
        }

        public async Task DeleteAsync(string key)
        {
            await _database.KeyDeleteAsync(key);
        }

        public Task<RedisValue> GetAsync(string key)
        {
            return _database.StringGetAsync(key);
        }
    }
}
