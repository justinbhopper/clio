using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public class MockContainerWriter<T> : IContainerWriter
    {
        private readonly ICollection<T> _target;
        private readonly JsonSerializer _serializer;

        public MockContainerWriter(ICollection<T> target)
            : this(target, JsonSerializer.CreateDefault()) { }

        public MockContainerWriter(ICollection<T> target, JsonSerializer serializer)
        {
            _target = target;
            _serializer = serializer;
        }

        public async Task RestoreAsync(IReceivableSourceBlock<JObject> documentsSource, CancellationToken cancellationToken = default)
        {
            while (await documentsSource.OutputAvailableAsync(cancellationToken))
            {
                var document = await documentsSource.ReceiveAsync(cancellationToken);
                _target.Add(document.ToObject<T>(_serializer));
            }
        }
    }
}
