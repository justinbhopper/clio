using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
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

        public async Task ReadDocumentsAsync(ITargetBlock<JObject> target, QueryDefinition query, CancellationToken cancellationToken = default)
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

                    if (!response.IsSuccessStatusCode)
                    {
                        if (response.StatusCode != HttpStatusCode.TooManyRequests)
                            throw new Exception($"Unexpected status code {response.StatusCode} in response."); // TODO: Create better exception

                        if (!response.Headers.TryGetValue("x-ms-retry-after-ms", out var waitTimeMs) || string.IsNullOrEmpty(waitTimeMs))
                            waitTimeMs = "100";

                        await Task.Delay(TimeSpan.FromMilliseconds(double.Parse(waitTimeMs)));

                        continue;
                    }

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
                            await target.SendAsync(document, cancellationToken);
                    }
                }
            }
            while (!string.IsNullOrEmpty(continuationToken));

            target.Complete();
        }
    }
}
