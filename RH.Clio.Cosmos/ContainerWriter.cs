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

        public async Task RestoreAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken = default)
        {
            var container = await _destination.ReadContainerAsync(cancellationToken: cancellationToken);
            var partitionKeyPath = container.Resource.PartitionKeyPath?
                .Split('/')
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList() ?? new List<string>();

            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                var json = document.ToString(Formatting.None);

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

                var partitionKeyValue = GetPartitionKey(document);

                var partitionKey = !string.IsNullOrEmpty(partitionKeyValue)
                    ? new PartitionKey(partitionKeyValue)
                    : PartitionKey.None;

                var response = await _destination.UpsertItemStreamAsync(stream, partitionKey, cancellationToken: cancellationToken);

                _logger.LogTrace("Inserted document, cost {requestCharge} RUs.");
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
    }
}
