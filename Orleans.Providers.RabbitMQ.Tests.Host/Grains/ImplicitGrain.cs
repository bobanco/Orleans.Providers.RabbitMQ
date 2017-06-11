using Orleans.Providers.RabbitMQ.Tests.Host.Interfaces;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Tests.Host.Grains
{
    [ImplicitStreamSubscription("TestNamespace")]
    public class ImplicitGrain : Grain, IImplicitGrain, IAsyncObserver<string>
    {
        private StreamSubscriptionHandle<string> _subscription;

        public override async Task OnActivateAsync()
        {
            var grainId = this.GetPrimaryKey();
            var provider = GetStreamProvider("Default");
            var stream = provider.GetStream<string>(grainId, "TestNamespace");
            _subscription = await stream.SubscribeAsync(this);
            GetLogger().Info("ImplicitGrainId: {0}", grainId);
        }

        public Task OnCompletedAsync()
        {
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {
            return TaskDone.Done;
        }

        public Task OnNextAsync(string item, StreamSequenceToken token = null)
        {
            GetLogger().Info("Received message '{0}'!", item);
            return TaskDone.Done;
        }
    }
}
