using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public class ContainerReader : IContainerReader
    {
        private readonly Container _container;

        public ContainerReader(Container container)
        {
            _container = container;
        }

        public async IAsyncEnumerable<JObject> GetDocumentsAsync(QueryDefinition query, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            string? continuationToken = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var iterator = _container.GetItemQueryStreamIterator(query, continuationToken);

                while (iterator.HasMoreResults)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using var response = await iterator.ReadNextAsync(cancellationToken);
                    using var reader = new StreamReader(response.Content);
                    using var jsonReader = new JsonTextReader(reader);

                    continuationToken = response.ContinuationToken;

                    var jToken = JToken.ReadFrom(jsonReader);
                    if (!(jToken is JObject jObject))
                        continue;

                    if (!(jObject.SelectToken("Documents") is JArray jArray))
                        continue;

                    foreach (var item in jArray.Children())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (item.Type == JTokenType.Object && item is JObject document)
                            yield return document;
                    }
                }
            }
            while (!string.IsNullOrEmpty(continuationToken));
        }
    }
}
