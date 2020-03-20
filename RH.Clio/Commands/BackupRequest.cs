using Microsoft.Azure.Cosmos;
using RH.Clio.Snapshots;

namespace RH.Clio.Commands
{
    public class BackupRequest : IRequest
    {
        public BackupRequest(string databaseName, string containerName, ISnapshotHandle destination, QueryDefinition documentsQuery)
        {
            DatabaseName = databaseName;
            ContainerName = containerName;
            Destination = destination;
            DocumentsQuery = documentsQuery;
        }

        public string DatabaseName { get; }

        public string ContainerName { get; }

        public ISnapshotHandle Destination { get; }

        public QueryDefinition DocumentsQuery { get; }
    }
}
