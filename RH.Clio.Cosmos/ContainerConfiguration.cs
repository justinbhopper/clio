namespace RH.Clio.Cosmos
{
    public class ContainerConfiguration
    {
        public ContainerConfiguration(string name, string partitionKeyPath, int throughput)
        {
            Name = name;
            PartitionKeyPath = partitionKeyPath;
            Throughput = throughput;
        }

        public string Name { get; }

        public string PartitionKeyPath { get; }

        public int Throughput { get; }
    }
}
