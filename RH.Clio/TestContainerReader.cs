using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using RH.Clio.Cosmos;

namespace RH.Clio
{
    public class TestContainerReader : IContainerReader
    {
        private readonly IContainerReader _decorated;
        private readonly Container _container;

        public TestContainerReader(
            IContainerReader decorated,
            Container container)
        {
            _decorated = decorated;
            _container = container;
        }

        public async IAsyncEnumerable<JObject> GetDocuments(QueryDefinition query, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var document in _decorated.GetDocuments(new QueryDefinition(string.Empty), cancellationToken))
            {
                await _container.UpsertItemAsync(new Test(), cancellationToken: cancellationToken);

                yield return document;
            }
        }

        private class Test
        {
            public string Id { get; } = Guid.NewGuid().ToString();
        }
    }
}
