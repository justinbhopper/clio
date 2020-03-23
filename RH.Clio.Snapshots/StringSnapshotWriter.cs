using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RH.Clio.Snapshots
{
    public abstract class StringSnapshotWriter : ISnapshotWriter
    {
        protected abstract Task AppendDocumentsAsync(IReceivableSourceBlock<string> documents, CancellationToken cancellationToken);

        protected abstract Task AppendDocumentsAsync(IEnumerable<string> documents, CancellationToken cancellationToken);

        public virtual void Close()
        {
            // Do nothing by default
        }

        public abstract ValueTask DisposeAsync();

        public async Task AppendDocumentsAsync(IReceivableSourceBlock<JObject> documents, CancellationToken cancellationToken)
        {
            var converter = new TransformBlock<JObject, string>(Serialize);

            var filter = new BufferBlock<string>();
            converter.LinkTo(filter, new DataflowLinkOptions
            {
                PropagateCompletion = true
            }, d => !string.IsNullOrEmpty(d));

            // Discard invalid blobs
            converter.LinkTo(DataflowBlock.NullTarget<string>());

            documents.LinkTo(converter, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });

            await AppendDocumentsAsync(filter, cancellationToken);
            await Task.WhenAll(converter.Completion, filter.Completion, documents.Completion);
        }

        public async Task AppendDocumentsAsync(IEnumerable<JObject> documents, CancellationToken cancellationToken)
        {
            await AppendDocumentsAsync(SerializeDocuments(documents), cancellationToken);
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
