using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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
    public class ConcurrentContainerWriter : IContainerWriter, IDocumentWriter
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

        public event EventHandler<DocumentEventArgs>? ThrottleWaitStarted;

        public event EventHandler<DocumentEventArgs>? ThrottleWaitFinished;

        public event EventHandler<DocumentEventArgs>? DocumentQueued;

        public event EventHandler<DocumentEventArgs>? DocumentInserting;

        public event EventHandler<DocumentEventArgs>? DocumentInserted;

        public event EventHandler<DocumentEventArgs>? DocumentFailed;

        public async Task RestoreAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken = default)
        {
            var container = await _destination.ReadContainerAsync(cancellationToken: cancellationToken);
            var partitionKeyPath = container.Resource.PartitionKeyPath?
                .Split('/')
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList() ?? new List<string>();

            // http://blog.i3arnon.com/2016/05/23/tpl-dataflow/

            // TODO: Make configurable
            // This is the max items allowed to be queued up in memory
            var boundedCapacity = 50;

            var throttledQueue = new TransformBlock<(TimeSpan wait, DocumentItem document), DocumentItem>(
                async (item) =>
                {
                    ThrottleWaitStarted?.Invoke(this, item.document);
                    _logger.LogTrace("Waiting {waitTimeMs} before retrying...", item.wait.TotalMilliseconds);
                    await Task.Delay(item.wait);
                    ThrottleWaitFinished?.Invoke(this, item.document);
                    return item.document;
                },
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = false,
                    MaxDegreeOfParallelism = boundedCapacity, // Limit how many concurrent items can be waiting
                    BoundedCapacity = boundedCapacity
                });

            // Use ActionBlock to throttle number of upserts occurring concurrently
            var upsertBlock = new ActionBlock<DocumentItem>(
                Upsert,
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    MaxDegreeOfParallelism = boundedCapacity,
                    BoundedCapacity = boundedCapacity, 
                    CancellationToken = cancellationToken
                });

            throttledQueue.LinkTo(upsertBlock, new DataflowLinkOptions
            {
                PropagateCompletion = false
            });

            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                var item = new DocumentItem(document);
                DocumentQueued?.Invoke(this, item);

                // Queue up the document
                await upsertBlock.SendAsync(item, cancellationToken);

                _logger.LogTrace("Queued up {documentCount} documents...", upsertBlock.InputCount);
            }

            // Notify completion of all input
            throttledQueue.Complete(); // Is this right? Do we know we've completed throttle queuing yet?
            upsertBlock.Complete();

            // Wait for all upserts to complete
            await Task.WhenAll(throttledQueue.Completion, upsertBlock.Completion);

            async Task Upsert(DocumentItem item)
            {
                var document = item.JObject;
                var json = document.ToString(Formatting.None);

                var partitionKeyValue = GetPartitionKey(document);

                var partitionKey = !string.IsNullOrEmpty(partitionKeyValue)
                    ? new PartitionKey(partitionKeyValue)
                    : PartitionKey.None;

                using var content = new MemoryStream(Encoding.UTF8.GetBytes(json));

                DocumentInserting?.Invoke(this, item);
                _logger.LogTrace("Inserting document {activityId}...");

                var response = await _destination.UpsertItemStreamAsync(content, partitionKey, cancellationToken: cancellationToken);

                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode != HttpStatusCode.TooManyRequests)
                    {
                        // TODO: Add more detail to event
                        DocumentFailed?.Invoke(this, item);
                        _logger.LogTrace("Failed to insert document.  {statusCode} - {errorMessage}", response.StatusCode, response.ErrorMessage);
                    }
                    else
                    {
                        if (!response.Headers.TryGetValue("x-ms-retry-after-ms", out var waitTimeMs) || string.IsNullOrEmpty(waitTimeMs))
                            waitTimeMs = "100";

                        await throttledQueue.SendAsync((TimeSpan.FromMilliseconds(double.Parse(waitTimeMs)), item));
                    }
                }
                else
                {
                    DocumentInserted?.Invoke(this, item);
                    _logger.LogTrace("Inserted document, cost {requestCharge} RUs.", response.Headers.RequestCharge);
                }
            }

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

        private class DocumentItem : DocumentEventArgs
        {
            public DocumentItem(JObject jObject)
            {
                JObject = jObject;
            }

            public JObject JObject { get; }
        }
    }
}
