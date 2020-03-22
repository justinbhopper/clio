using Microsoft.Azure.Cosmos;

namespace RH.Clio
{
    public interface ICosmosClientFactory
    {
        CosmosClient CreateClient(bool allowBulkExecution);
    }
}