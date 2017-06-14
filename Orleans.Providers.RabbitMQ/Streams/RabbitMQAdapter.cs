using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Streams
{
    internal class RabbitMQAdapter<TDataAdapter> : IQueueAdapter
        where TDataAdapter : IRabbitMQDataAdapter
    {
        private readonly TDataAdapter _dataAdapter;
        private readonly SerializationManager _serializationManager;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly RabbitMQStreamProviderConfiguration _configuration;
        private readonly Logger _logger;
        protected readonly ConcurrentDictionary<QueueId, RabbitMQMessageQueueDataManager> Queues = new ConcurrentDictionary<QueueId, RabbitMQMessageQueueDataManager>();
        public string Name { get; }
        public bool IsRewindable => false;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public RabbitMQAdapter(TDataAdapter dataAdapter,
            SerializationManager serializationManager,
            HashRingBasedStreamQueueMapper streamQueueMapper,
            RabbitMQStreamProviderConfiguration configuration,
            string providerName,
            Logger logger)
        {
            _dataAdapter = dataAdapter;
            _serializationManager = serializationManager;
            _streamQueueMapper = streamQueueMapper;
            _configuration = configuration;
            _logger = logger;
            Name = providerName;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            RabbitMQMessageQueueDataManager queue;
            if (!Queues.TryGetValue(queueId, out queue))
            {
                var tmpQueue =
                    RabbitMQResourceManager.CreateQueueDataManager(Name, _configuration, $"{Name}_Producer", _logger);
                await tmpQueue.InitQueueAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }
            var rabbitMsg = _dataAdapter.ToRabbitMQMessage(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(rabbitMsg);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RabbitMQAdapterReceiver.Create(_configuration, Name,_serializationManager, queueId,
                _dataAdapter, _logger);
        }

        
    }
}
