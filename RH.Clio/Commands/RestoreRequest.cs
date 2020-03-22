using System;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots;

namespace RH.Clio.Commands
{
    public class RestoreRequest : IRequest, IDocumentWriter
    {
        public RestoreRequest(string databaseName, ContainerConfiguration containerConfig, ISnapshotReader source)
        {
            DatabaseName = databaseName;
            ContainerConfiguration = containerConfig;
            Source = source;
        }

        public event EventHandler<DocumentEventArgs>? ThrottleWaitStarted;
        public event EventHandler<DocumentEventArgs>? ThrottleWaitFinished;
        public event EventHandler<DocumentEventArgs>? DocumentQueued;
        public event EventHandler<DocumentEventArgs>? DocumentInserting;
        public event EventHandler<DocumentEventArgs>? DocumentInserted;
        public event EventHandler<DocumentEventArgs>? DocumentFailed;

        public string DatabaseName { get; }

        public ContainerConfiguration ContainerConfiguration { get; }

        public ISnapshotReader Source { get; }

        public bool DropContainerIfExists { get; set; }

        public void OnDocumentInserted(DocumentEventArgs e) => DocumentInserted?.Invoke(this, e);
        public void OnDocumentInserting(DocumentEventArgs e) => DocumentInserting?.Invoke(this, e);
        public void OnDocumentQueued(DocumentEventArgs e) => DocumentQueued?.Invoke(this, e);
        public void OnDocumentFailed(DocumentEventArgs e) => DocumentFailed?.Invoke(this, e);
        public void OnThrottleWaitStarted(DocumentEventArgs e) => ThrottleWaitStarted?.Invoke(this, e);
        public void OnThrottleWaitFinished(DocumentEventArgs e) => ThrottleWaitFinished?.Invoke(this, e);
    }
}
