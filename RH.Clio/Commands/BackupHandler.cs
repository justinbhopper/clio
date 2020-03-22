using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using RH.Clio.Cosmos;

namespace RH.Clio.Commands
{
    public class BackupHandler : IRequestHandler<BackupRequest>
    {
        private readonly CosmosClient _cosmosClient;

        public BackupHandler(ICosmosClientFactory clientFactory)
        {
            _cosmosClient = clientFactory.CreateClient(false);
        }

        public async Task Handle(BackupRequest request, CancellationToken cancellationToken)
        {
            var database = _cosmosClient.GetDatabase(request.DatabaseName);
            var leaseContainerName = request.ContainerName + "-lease";

            var sourceContainer = database.GetContainer(request.ContainerName);

            // TODO: Should we own the lease container?  Leaving it around sometimes seems to cause issues with future runs...
            await database.GetContainer(leaseContainerName).DeleteContainerIfExistsAsync(cancellationToken);

            try
            {
                var leaseContainerProperties = new ContainerProperties(leaseContainerName, "/id");
                var leaseContainer = await database.CreateContainerIfNotExistsAsync(leaseContainerProperties, throughput: 9000);

                var containerReader = new ContainerReader(sourceContainer);
                var processor = new SnapshotProcessor(sourceContainer, leaseContainer, request.Destination, request.DocumentsQuery, containerReader);

                await processor.StartAsync(cancellationToken);
                await processor.WaitAsync(cancellationToken);
            }
            finally
            {
                await database.GetContainer(leaseContainerName).DeleteContainerIfExistsAsync(cancellationToken);
            }
        }
    }
}
