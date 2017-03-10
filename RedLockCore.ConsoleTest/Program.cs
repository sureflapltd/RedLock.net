using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedLock;

namespace RedLockCore.ConsoleTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
	        MainAsync().Wait();
        }

	    public static async Task MainAsync()
	    {
			var loggerFactory = new LoggerFactory();
			loggerFactory.AddConsole(LogLevel.Debug);

		    var logger = loggerFactory.CreateLogger<Program>();
			logger.LogDebug("Starting");

		    var endpoints = new List<EndPoint> {new DnsEndPoint("localhost", 6379)};

			using (var redisLockFactory = new RedisLockFactory(endpoints, loggerFactory))
			{
				var resource = $"testredislock-{Guid.NewGuid()}";

				using (var firstLock = await redisLockFactory.CreateAsync(resource, TimeSpan.FromSeconds(30)))
				{
					if (!firstLock.IsAcquired)
					{
						throw new Exception("First lock not acquired");
					}

					using (var secondLock = await redisLockFactory.CreateAsync(resource, TimeSpan.FromSeconds(30)))
					{
						if (secondLock.IsAcquired)
						{
							throw new Exception("Second lock acquired");
						}
					}
				}
			}
		}
    }
}
