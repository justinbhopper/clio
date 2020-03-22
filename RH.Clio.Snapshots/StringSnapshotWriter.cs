using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public abstract class StringSnapshotWriter : ISnapshotWriter
    {
        protected abstract Task AppendDocumentsAsync(IAsyncEnumerable<string> documents, CancellationToken cancellationToken);

        protected abstract Task AppendDocumentsAsync(IEnumerable<string> documents, CancellationToken cancellationToken);

        public virtual void Close()
        {
            // Do nothing by default
        }

        public abstract ValueTask DisposeAsync();

        public async Task AppendDocumentsAsync(IAsyncEnumerable<JObject> documents, CancellationToken cancellationToken)
        {
            await AppendDocumentsAsync(SerializeDocuments(documents, cancellationToken), cancellationToken);
        }

        public async Task AppendDocumentsAsync(IEnumerable<JObject> documents, CancellationToken cancellationToken)
        {
            await AppendDocumentsAsync(SerializeDocuments(documents), cancellationToken);
        }

        private async IAsyncEnumerable<string> SerializeDocuments(IAsyncEnumerable<JObject> documents, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var document in documents.WithCancellation(cancellationToken))
            {
                var serialized = Serialize(document);
                if (!string.IsNullOrWhiteSpace(serialized))
                    yield return serialized;
            }
        }

        private IEnumerable<string> SerializeDocuments(IEnumerable<JObject> documents)
        {
            foreach (var document in documents)
            {
                var serialized = Serialize(document);
                if (!string.IsNullOrWhiteSpace(serialized))
                    yield return serialized;
            }
        }

        private static string Serialize(JObject document)
        {
            return document.ToString(Formatting.None);
        }
    }
}
