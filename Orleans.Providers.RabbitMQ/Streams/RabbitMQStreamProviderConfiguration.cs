using System;
using RabbitMQ.Client;

namespace Orleans.Providers.RabbitMQ.Streams
{
    [Serializable]
    public sealed class RabbitMQStreamProviderConfiguration
    {
        public string DeployementId { get; }
        public int NumQueues { get; }
        public string Exchange { get; }
        public string ExchangeType { get; }
        public bool ExchangeDurable { get; }
        public bool AutoDelete { get; }
        public string Queue { get; }
        public bool QueueDurable { get; }
        public string RoutingKey { get; }
        public string Uri { get; }

        public TimeSpan QueueOperationTimeout { get; }

        public RabbitMQStreamProviderConfiguration(IProviderConfiguration config)
        {
            string deployementId;
            if (!config.Properties.TryGetValue("DeploymentId", out deployementId))
                throw new ArgumentException("DeploymentId property not set");
            DeployementId = deployementId;
            NumQueues = config.GetIntProperty("NumQueues", 8);
            Exchange = config.Properties["Exchange"];
            ExchangeType = config.GetProperty("ExchangeType", "Direct").ToLowerInvariant();
            ExchangeDurable = config.GetBoolProperty("ExchangeDurable", false);
            AutoDelete = config.GetBoolProperty("AutoDelete", false);
            Queue = config.Properties["Queue"];
            QueueDurable = config.GetBoolProperty("QueueDurable", false);
            RoutingKey = config.Properties["RoutingKey"];
            QueueOperationTimeout = config.GetTimeSpanProperty("QueueOperationTimeout", TimeSpan.FromSeconds(15));
            Uri = config.GetProperty("Uri", null);
        }

        public IConnectionFactory ToConnectionFactory()
        {
            var factory = new ConnectionFactory();
            if (!string.IsNullOrWhiteSpace(Uri))
            {
                factory.Uri = Uri;
                return factory;
            }
            factory.AutomaticRecoveryEnabled = true;//enable auto receonnect
            factory.TopologyRecoveryEnabled = true;//enable auto tolopology recovery
            return factory;
        }
        
    }
}
