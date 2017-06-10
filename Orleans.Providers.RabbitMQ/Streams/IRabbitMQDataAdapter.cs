using Orleans.Streams;
using System;
using System.Collections.Generic;

namespace Orleans.Providers.RabbitMQ.Streams
{
    public interface IRabbitMQDataAdapter
    {
        /// <summary>
        /// Creates a rabbitmq queue message from stream event data.
        /// </summary>
        /// <typeparam name="T">the type of the events</typeparam>
        /// <param name="streamGuid">the stream id</param>
        /// <param name="streamNamespace">the stream namespace</param>
        /// <param name="events">the events in the batch</param>
        /// <param name="requestContext">the request context</param>
        /// <returns></returns>
        RabbitMQMessage ToRabbitMQMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events,
            Dictionary<string, object> requestContext);

        /// <summary>
        /// Creates a batch container from a cloud queue message
        /// </summary>
        IBatchContainer FromRabbitMQMessage(RabbitMQMessage message, long sequenceId);
    }
}
