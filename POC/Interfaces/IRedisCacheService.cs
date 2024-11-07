using StackExchange.Redis;

namespace POC.Interfaces
{
    public interface IRedisCacheService
    {
        Task<bool> ExistsAsync(string key);
        Task SetAsync(string key, object value);
        Task DeleteAsync(string key);
        Task<RedisValue> GetAsync(string key);
    }
}
