using System;
using RabbitMQ.Client;

namespace Orleans.Providers.RabbitMQ.Streams
{
    /// <summary>
    /// Represent rabbitmq message
    /// </summary>
    [Serializable]
    public sealed class RabbitMQMessage
    {
        
        /// <summary>
        /// Gets the body of the message
        /// </summary>
        public byte[] Body { get; }

        /// <summary>
        /// Gets the delivery tag for this message. See also <see cref="M:RabbitMQ.Client.IModel.BasicAck(System.UInt64,System.Boolean)"/>.
        /// </summary>
        public ulong DeliveryTag { get; }

        /// <summary>
        /// Retrieve the redelivered flag of this message.
        /// </summary>
        public bool IsRedelivered { get; }

        /// <summary>
        /// Retrive the exchange this message was published.
        /// </summary>
        public string Exchange { get; }
        /// <summary>
        /// Retrive the routing key with which this message was published.
        /// </summary>
        public string RoutingKey { get; }

        /// <summary>
        /// Retrive the content header properties of this message.
        /// </summary>
        public IBasicProperties Properties { get; }

        public RabbitMQMessage(byte[] body, ulong deliveryTag, bool isRedelivered, string exchange, string routingKey, IBasicProperties properties)
            :this(body, deliveryTag)
        {
            IsRedelivered = isRedelivered;
            Exchange = exchange;
            RoutingKey = routingKey;
            Properties = properties;
        }

        public RabbitMQMessage(byte[] body, ulong deliveryTag)
            :this(body)
        {
           DeliveryTag = deliveryTag;
        }

        public RabbitMQMessage(byte[] body)
        {
            Body = body;
        }
    }
}
