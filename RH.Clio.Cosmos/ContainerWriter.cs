using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public class ContainerWriter : IContainerWriter
    {
        private readonly Container _destination;
        private readonly ILogger<ContainerWriter> _logger;

        public ContainerWriter(
            Container destination,
            ILogger<ContainerWriter> logger)
        {
            _destination = destination;
            _logger = logger;
        }

        public async Task RestoreAsync(IAsyncEnumerable<JObject> documents, bool concurrent, CancellationToken cancellationToken = default)
        {
            var container = await _destination.ReadContainerAsync(cancellationToken: cancellationToken);
            var partitionKeyPath = container.Resource.PartitionKeyPath?
                .Split('/')
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList() ?? new List<string>();

            var tasks = new List<Task>();

            // TODO: Create batches or producer/consumer here to prevent flooding memory with tasks

            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                var json = document.ToString(Formatting.None);

                var partitionKeyValue = GetPartitionKey(document);

                var partitionKey = !string.IsNullOrEmpty(partitionKeyValue)
                    ? new PartitionKey(partitionKeyValue)
                    : PartitionKey.None;

                // Queue up inserts to run concurrently to take advantage of bulk execution support
                var task = Task.Run(async () =>
                {
                    using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
                    var response = await _destination.UpsertItemStreamAsync(stream, partitionKey, cancellationToken: cancellationToken);

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogError("Failed to insert document, received status code {statusCode}", response.StatusCode);
                    }
                    else
                    {
                        _logger.LogTrace("Inserted document, cost {requestCharge} RUs.", response.Headers.RequestCharge);
                    }
                });

                tasks.Add(task);

                if (!concurrent)
                    await task;
            }

            await Task.WhenAll(tasks);

            string GetPartitionKey(JObject document)
            {
                for (var i = 0; i < partitionKeyPath.Count; i++)
                {
                    var path = partitionKeyPath[i];
                    var value = document[path];

                    if (value is null)
                        break;

                    if (value is JObject jObject)
                        document = jObject;
                    else if (i == (partitionKeyPath.Count - 1) && value is JValue jValue)
                        return jValue.ToString();
                }

                return string.Empty;
            }
        }
    }
    /*
    internal interface IDocumentWriter
    {
        Task UpsertDocumentAsync(JObject document, CancellationToken cancellationToken = default);
    }

    internal class DocumentWriter : IDocumentWriter
    {
        private IList<string>? _partitionKeyPaths;

        public DocumentWriter(Container destinationContainer)
        {

        }

        private IList<string> 
    }*/
}
