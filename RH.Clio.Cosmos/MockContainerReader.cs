using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public class MockContainerReader<T> : IContainerReader
    {
        private readonly IEnumerable<T> _source;
        private readonly JsonSerializer _serializer;

        public MockContainerReader(IEnumerable<T> source)
            : this(source, JsonSerializer.CreateDefault()) { }

        public MockContainerReader(IEnumerable<T> source, JsonSerializer serializer)
        {
            _source = source;
            _serializer = serializer;
        }

        public async Task ReadDocumentsAsync(ITargetBlock<JObject> target, QueryDefinition query, CancellationToken cancellationToken = default)
        {
            foreach (var item in _source)
            {
                if (item is null)
                    continue;

                await target.SendAsync(JObject.FromObject(item, _serializer), cancellationToken);
            }

            target.Complete();
        }
    }
}
