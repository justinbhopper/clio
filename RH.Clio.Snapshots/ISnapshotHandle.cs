using System.Threading;
using System.Threading.Tasks;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotHandle : ISnapshotWriter
    {
        Task DeleteAsync(CancellationToken cancellationToken);
    }
}
