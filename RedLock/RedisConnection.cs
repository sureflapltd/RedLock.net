using StackExchange.Redis;

namespace RedLock
{
	internal class RedisConnection
	{
		public IConnectionMultiplexer ConnectionMultiplexer { get; set; }
		public int RedisDatabase { get; set; }
		public string RedisKeyFormat { get; set; }
	}
}