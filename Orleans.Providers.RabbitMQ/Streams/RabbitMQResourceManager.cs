using Orleans.Runtime;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Streams
{

    public static class RabbitMQResourceManager
    {
        private static readonly ConcurrentDictionary<string, List<RabbitMQMessageQueueDataManager>> QueueDataManagers = new ConcurrentDictionary<string, List<RabbitMQMessageQueueDataManager>>();
        public static RabbitMQMessageQueueDataManager CreateQueueDataManager(string providerName,RabbitMQStreamProviderConfiguration configuration, string connectionName,
            Logger logger)
        {
            var queue = new RabbitMQMessageQueueDataManager(configuration, connectionName, logger);
            List<RabbitMQMessageQueueDataManager> queues;
            if (QueueDataManagers.TryGetValue(providerName, out queues))
            {
                queues.Add(queue);
            }
            else
            {
                queues = new List<RabbitMQMessageQueueDataManager> { queue };
                QueueDataManagers.TryAdd(providerName, queues);
            }
            return queue;
        }

        public static Task Shutdown(string providerName)
        {
            List<RabbitMQMessageQueueDataManager> queues;
            if (QueueDataManagers.TryGetValue(providerName, out queues))
            {
                if (queues.Any())
                {
                    var closeTask = Task.WhenAll(queues.Select(queue => queue.CloseQueueAsync()));
                    queues.Clear();
                    return closeTask;
                }
            }
            return Task.CompletedTask;
        }
    }
}
