namespace RH.Clio.Cosmos
{
    public class ContainerConfiguration
    {
        public ContainerConfiguration(string databaseName, string containerName, string partitionKeyPath, int throughput)
        {
            DatabaseName = databaseName;
            ContainerName = containerName;
            PartitionKeyPath = partitionKeyPath;
            Throughput = throughput;
        }

        public string DatabaseName { get; }

        public string ContainerName { get; }

        public string PartitionKeyPath { get; }

        public int Throughput { get; }
    }
}
