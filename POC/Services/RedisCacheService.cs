using Newtonsoft.Json;
using POC.Interfaces;
using StackExchange.Redis;

namespace POC.Services
{
    public class RedisCacheService : IRedisCacheService
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly IDatabase _database;

        public RedisCacheService()
        {
            _redis = ConnectionMultiplexer.Connect("localhost:6379,password=Abc.2024");
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
