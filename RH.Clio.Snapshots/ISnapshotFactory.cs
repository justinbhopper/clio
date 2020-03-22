using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotFactory
    {
        ISnapshotReader CreateReader();

        Task<ISnapshotHandle> CreateWriterAsync(bool deleteIfExists, CancellationToken cancellationToken);
    }
}
