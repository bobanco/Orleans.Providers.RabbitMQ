using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Streams
{
    /// <summary>
    /// factory class for RabbitMQ based stream provider
    /// </summary>
    public class RabbitMQAdapterFactory<TDataAdapter> : IQueueAdapterFactory
        where TDataAdapter : IRabbitMQDataAdapter
    {
        private int _cacheSize;
        private RabbitMQStreamProviderConfiguration _configuration;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private Func<TDataAdapter> _adaptorFactory;
        private Logger _logger;
        private string _providerName;

        /// <summary>
        /// Gets the serialization manager.
        /// </summary>
        public SerializationManager SerializationManager { get; private set; }

        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }
        public void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _configuration = new RabbitMQStreamProviderConfiguration(config);
            _logger = logger;
            _providerName = providerName;

            _cacheSize = SimpleQueueAdapterCache.ParseSize(config, 4096);

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_configuration.NumQueues, providerName);
            _adapterCache = new SimpleQueueAdapterCache(_cacheSize, logger);

            if (StreamFailureHandlerFactory == null)
            {
                StreamFailureHandlerFactory =
                    qid => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));
            }

            this.SerializationManager = serviceProvider.GetRequiredService<SerializationManager>();
            _adaptorFactory = () => ActivatorUtilities.GetServiceOrCreateInstance<TDataAdapter>(serviceProvider);

        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RabbitMQAdapter<TDataAdapter>(_adaptorFactory(), SerializationManager, _streamQueueMapper,
                _configuration, _providerName, _logger);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

        public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
            StreamFailureHandlerFactory(queueId);
    }
}
