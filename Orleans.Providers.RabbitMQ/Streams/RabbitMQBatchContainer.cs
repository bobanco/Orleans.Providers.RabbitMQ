using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Providers.RabbitMQ.Streams
{
    [Serializable]
    public class RabbitMQBatchContainer : IBatchContainer
    {
        [JsonProperty]
        private readonly Dictionary<string, object> _requestContext;
        [JsonProperty]
        private EventSequenceTokenV2 _sequenceToken;

        private readonly List<object> _events;
        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken SequenceToken => _sequenceToken;

        internal EventSequenceTokenV2 RealSequenceToken
        {
            set => _sequenceToken = value;
        }

        public RabbitMQBatchContainer(
            Guid streamGuid,
            String streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext,
            EventSequenceTokenV2 sequenceToken)
            : this(streamGuid, streamNamespace, events, requestContext)
        {
            _sequenceToken = sequenceToken;
        }

        public RabbitMQBatchContainer(Guid streamGuid, String streamNamespace, List<object> events, Dictionary<string, object> requestContext)
        {
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
            _requestContext = requestContext;
        }


        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, _sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContext.Import(_requestContext);
                return true;
            }
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            // maybe there is something in this batch that the consumer is intereted in, so we should send it.
            return _events.Any(item => shouldReceiveFunc(stream, filterData, item));
           
        }

        public override string ToString()
        {
            return $"[RabbitMQBatchContainer:Stream={StreamGuid},#Items={_events.Count}]";
        }
    }
}
