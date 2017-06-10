using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Streams
{
    /// <summary>
    /// Recieves batches of messages from a single partition of a message queue.  
    /// </summary>
    internal class RabbitMQAdapterReceiver : IQueueAdapterReceiver
    {
       
        private readonly RabbitMQStreamProviderConfiguration _configuration;
        private readonly SerializationManager _serializationManager;
        private RabbitMQMessageQueueDataManager _queue;
        private readonly IRabbitMQDataAdapter _dataAdapter;
        private readonly List<PendingDelivery> _pending;
        private long _lastReadMessage;
        private Task _outstandingTask;
        private const int MaxNumberOfMessagesToPeek = 32;
        private readonly Logger _logger;

        public QueueId Id { get; }

        public static RabbitMQAdapterReceiver Create(RabbitMQStreamProviderConfiguration configuration,
            SerializationManager serializationManager, QueueId queueId, IRabbitMQDataAdapter dataAdapter, IConnection connection, Logger logger)
        {
            if(configuration==null)throw new ArgumentNullException(nameof(configuration));
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));
            if (serializationManager == null) throw new ArgumentNullException(nameof(serializationManager));
            if(connection==null) throw new ArgumentNullException(nameof(connection));
            if(logger == null) throw new ArgumentNullException(nameof(logger));
            var queue = new RabbitMQMessageQueueDataManager(configuration, connection, logger);
            return new RabbitMQAdapterReceiver(configuration, serializationManager, queueId, queue, dataAdapter, logger);
        }

        public RabbitMQAdapterReceiver(RabbitMQStreamProviderConfiguration configuration, SerializationManager serializationManager, QueueId queueId, RabbitMQMessageQueueDataManager queue, IRabbitMQDataAdapter dataAdapter, Logger logger)
        {
            _configuration = configuration;
            _serializationManager = serializationManager;
            _queue = queue;
            _dataAdapter = dataAdapter;
            Id = queueId;
            _logger = logger;
            _pending = new List<PendingDelivery>();

        }

        public Task Initialize(TimeSpan timeout)
        {
            if (_queue != null) // check in case we already shut it down.
            {
                return _queue.InitQueueAsync();
            }
            return TaskDone.Done;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (queueRef == null) return new List<IBatchContainer>();
                var count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ?
                    MaxNumberOfMessagesToPeek : Math.Min(maxCount, MaxNumberOfMessagesToPeek);
                var task = queueRef.GetQueueMessages(count);
                _outstandingTask = task;
                var messages = await task;
                var rabbitMessages = messages
                    .Select(msg =>
                    {
                        var container = _dataAdapter.FromRabbitMQMessage(msg, _lastReadMessage++);
                        _pending.Add(new PendingDelivery(container.SequenceToken, msg));
                        return container;
                    }).ToList();
                return rabbitMessages;
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
                if (messages.Count == 0 || queueRef == null) return;
                // get sequence tokens of delivered messages
                List<StreamSequenceToken> deliveredTokens = messages.Select(message => message.SequenceToken).ToList();
                // find oldest delivered message
                StreamSequenceToken oldest = deliveredTokens.Max();
                // finalize all pending messages at or befor the oldest
                List<PendingDelivery> finalizedDeliveries = _pending
                    .Where(pendingDelivery => !pendingDelivery.Token.Newer(oldest))
                    .ToList();
                if (finalizedDeliveries.Count == 0) return;
                // remove all finalized deliveries from pending, regardless of if it was delivered or not.
                _pending.RemoveRange(0, finalizedDeliveries.Count);
                // get the queue messages for all finalized deliveries that were delivered.
                List<RabbitMQMessage> deliveredRabbitQueueMessages = finalizedDeliveries
                    .Where(finalized => deliveredTokens.Contains(finalized.Token))
                    .Select(finalized => finalized.Message)
                    .ToList();
                if (deliveredRabbitQueueMessages.Count == 0) return;
                // delete all delivered queue messages from the queue.  Anything finalized but not delivered will show back up later
                _outstandingTask = Task.WhenAll(deliveredRabbitQueueMessages.Select(queueRef.DeleteQueueMessage));
                try
                {
                    await _outstandingTask;
                }
                catch (Exception exc)
                {
                    _logger.Warn(100000,
                        $"Exception upon DeleteQueueMessage on queue {Id}. Ignoring.", exc);
                }
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                if (_outstandingTask != null) // await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
                    await _outstandingTask;
            }
            finally
            {
                _queue = null;  // remember that we shut down so we never try to read from the queue again.
            }
        }

        private class PendingDelivery
        {
            public PendingDelivery(StreamSequenceToken token, RabbitMQMessage message)
            {
                this.Token = token;
                this.Message = message;
            }

            public RabbitMQMessage Message { get; }

            public StreamSequenceToken Token { get; }
        }
    }
}
