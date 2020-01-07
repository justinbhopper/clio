using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Cosmos
{
    public class ConcurrentContainerWriter : IContainerWriter
    {
        private readonly Container _destination;
        private readonly ILogger<ConcurrentContainerWriter> _logger;

        public ConcurrentContainerWriter(
            Container destination,
            ILogger<ConcurrentContainerWriter> logger)
        {
            _destination = destination;
            _logger = logger;
        }

        public async Task RestoreAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken = default)
        {
            var container = await _destination.ReadContainerAsync(cancellationToken: cancellationToken);
            var partitionKeyPath = container.Resource.PartitionKeyPath?
                .Split('/')
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList() ?? new List<string>();

            // http://blog.i3arnon.com/2016/05/23/tpl-dataflow/

            // Use TransformBlock to prevent flooding memory with too many streams
            var encodeDocumentBlock = new TransformBlock<JObject, Document>(
                document =>
                {
                    var json = document.ToString(Formatting.None);

                    var partitionKeyValue = GetPartitionKey(document);

                    var partitionKey = !string.IsNullOrEmpty(partitionKeyValue)
                        ? new PartitionKey(partitionKeyValue)
                        : PartitionKey.None;

                    var content = new MemoryStream(Encoding.UTF8.GetBytes(json));
                    return new Document(partitionKey, content);
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                    BoundedCapacity = 100, // TODO: Make configurable, should be a value that prevents too much concurrent memory usage
                    CancellationToken = cancellationToken
                }
            );

            // Use ActionBlock to throttle number of upserts occurring concurrently
            var upsertBlock = new ActionBlock<Document>(
                async document =>
                {
                    try
                    {
                        // TODO: Add polly retry to prevent failure
                        // OR... requeue document into upsert block?
                        var response = await _destination.UpsertItemStreamAsync(document.Content, document.PartitionKey, cancellationToken: cancellationToken);

                        if (!response.IsSuccessStatusCode)
                        {
                            _logger.LogError("Failed to insert document, received status code {statusCode}", response.StatusCode);
                        }
                        else
                        {
                            _logger.LogTrace("Inserted document, cost {requestCharge} RUs.", response.Headers.RequestCharge);
                        }
                    }
                    finally
                    {
                        await document.Content.DisposeAsync();
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                    BoundedCapacity = 3, // TODO: This should be a value that prevents too many simultanious requests from upserting or waiting to retry
                    CancellationToken = cancellationToken
                });

            encodeDocumentBlock.LinkTo(upsertBlock, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });

            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                // Queue up the document
                await encodeDocumentBlock.SendAsync(document, cancellationToken);
            }

            // Notify completion of all input
            encodeDocumentBlock.Complete();

            // Wait for all upserts to complete
            await upsertBlock.Completion;

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

        private class Document
        {
            public Document(PartitionKey partitionKey, Stream content)
            {
                PartitionKey = partitionKey;
                Content = content;
            }

            public PartitionKey PartitionKey { get; }
            public Stream Content { get; }
        }
    }
}
