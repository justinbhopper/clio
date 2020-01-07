using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public abstract class StringSnapshotWriter : ISnapshotWriter
    {
        protected abstract Task AppendSnapshotDocumentAsync(string document, CancellationToken cancellationToken);

        protected abstract Task AppendChangeFeedDocumentAsync(string document, CancellationToken cancellationToken);

        public virtual void Close()
        {
            // Do nothing by default
        }

        public abstract ValueTask DisposeAsync();

        public async Task AppendSnapshotDocumentAsync(JObject document, CancellationToken cancellationToken)
        {
            var serialized = Serialize(document);
            if (!string.IsNullOrWhiteSpace(serialized))
                await AppendSnapshotDocumentAsync(serialized, cancellationToken);
        }

        public async Task AppendChangeFeedDocumentAsync(JObject document, CancellationToken cancellationToken)
        {
            var serialized = Serialize(document);
            if (!string.IsNullOrWhiteSpace(serialized))
                await AppendChangeFeedDocumentAsync(serialized, cancellationToken);
        }

        private static string Serialize(JObject document)
        {
            return document.ToString(Formatting.None);
        }
    }
}
