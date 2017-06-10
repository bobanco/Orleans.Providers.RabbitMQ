using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Providers.RabbitMQ.Streams
{
    /// <summary>
    /// Data adapter that uses types that support custom serializers (like json).
    /// </summary>
    public class RabbitMQDataAdapter : IRabbitMQDataAdapter, IOnDeserialized
    {
        private SerializationManager _serializationManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitMQDataAdapter"/> class.
        /// </summary>
        public RabbitMQDataAdapter(SerializationManager serializationManager)
        {
            _serializationManager = serializationManager;
        }

        public IBatchContainer FromRabbitMQMessage(RabbitMQMessage message, long sequenceId)
        {
            var rabbitMqQueueBatch = _serializationManager.DeserializeFromByteArray<RabbitMQBatchContainer>(message.Body);
            rabbitMqQueueBatch.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
            return rabbitMqQueueBatch;
        }

        public RabbitMQMessage ToRabbitMQMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var rabbitMqQueueBatch = new RabbitMQBatchContainer(streamGuid, streamNamespace,
                events.Cast<object>().ToList(), requestContext);
            var rawBytes = _serializationManager.SerializeToByteArray(rabbitMqQueueBatch);
            var rabbitMqMessage = new RabbitMQMessage(rawBytes);
            return rabbitMqMessage;
        }

        public void OnDeserialized(ISerializerContext context)
        {
            _serializationManager = context.SerializationManager;
        }
    }
}
