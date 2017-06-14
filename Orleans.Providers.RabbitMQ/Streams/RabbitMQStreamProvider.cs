using System.Threading.Tasks;
using Orleans.Providers.Streams.Common;

namespace Orleans.Providers.RabbitMQ.Streams
{
    /// <summary>
    /// Persistent stream provider that uses RabbitMQ queue for persistence
    /// </summary>
    public class RabbitMQStreamProvider : PersistentStreamProvider<RabbitMQAdapterFactory<RabbitMQDataAdapter>>, IProvider
    {
        async Task IProvider.Close()
        {
            await RabbitMQResourceManager.Shutdown(Name);
            await base.Close();
        }
    }
}
