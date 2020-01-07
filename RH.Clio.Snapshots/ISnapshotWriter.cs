using System;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public interface ISnapshotWriter : IAsyncDisposable
    {
        Task AppendSnapshotDocumentAsync(JObject document, CancellationToken cancellationToken);

        Task AppendChangeFeedDocumentAsync(JObject document, CancellationToken cancellationToken);

        void Close()
        {
            // No op by default
        }
    }
}
