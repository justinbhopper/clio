using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Cosmos
{
    public interface IContainerWriterFactory
    {
        Task<IContainerWriter> CreateWriterAsync(ContainerConfiguration config, bool dropContainerIfExists, CancellationToken cancellationToken);
    }
}
