using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using RH.Clio.Cosmos;

namespace RH.Clio.Commands
{
    public class RestoreHandler : IRequestHandler<RestoreRequest>
    {
        private readonly CosmosClient _cosmosClient;
        private readonly ILoggerFactory _loggerFactory;

        public RestoreHandler(CosmosClientFactory clientFactory, ILoggerFactory loggerFactory)
        {
            _cosmosClient = clientFactory.CreateClient(true);
            _loggerFactory = loggerFactory;
        }

        public async Task Handle(RestoreRequest request, CancellationToken cancellationToken)
        {
            var database = _cosmosClient.GetDatabase(request.DatabaseName);
            var containerConfig = request.ContainerConfiguration;

            if (request.DropContainerIfExists)
                await database.GetContainer(containerConfig.Name).DeleteContainerIfExistsAsync();

            var destinationContainerProperties = new ContainerProperties(containerConfig.Name, containerConfig.PartitionKeyPath);
            var destinationContainer = await database.CreateContainerIfNotExistsAsync(destinationContainerProperties, containerConfig.Throughput);
            var containerWriter = new ConcurrentContainerWriter(destinationContainer, _loggerFactory.CreateLogger<ConcurrentContainerWriter>());

            containerWriter.DocumentInserted += (s, e) => request.OnDocumentInserted(e);
            containerWriter.DocumentInserting += (s, e) => request.OnDocumentInserting(e);
            containerWriter.DocumentQueued += (s, e) => request.OnDocumentQueued(e);
            containerWriter.ThrottleWaitStarted += (s, e) => request.OnThrottleWaitStarted(e);
            containerWriter.ThrottleWaitFinished += (s, e) => request.OnThrottleWaitFinished(e);

            await containerWriter.RestoreAsync(request.Source);
        }
    }
}
