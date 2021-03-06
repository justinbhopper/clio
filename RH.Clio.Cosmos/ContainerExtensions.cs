using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Cosmos
{
    public static class ContainerExtensions
    {
        public static async Task DeleteContainerIfExistsAsync(this Container container, CancellationToken cancellationToken = default)
        {
            try
            {
                using (await container.DeleteContainerStreamAsync(cancellationToken: cancellationToken)) { }
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Ignore not found
            }
        }
    }
}
