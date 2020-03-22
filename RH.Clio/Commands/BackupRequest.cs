using System;
using Microsoft.Azure.Cosmos;
using RH.Clio.Cosmos;
using RH.Clio.Snapshots;

namespace RH.Clio.Commands
{
    public class BackupRequest : IRequest
    {
        public BackupRequest(string databaseName, string containerName, ISnapshotHandle destination, QueryDefinition documentsQuery)
        {
            DatabaseName = databaseName;
            ContainerName = containerName;
            Destination = destination;
            DocumentsQuery = documentsQuery;
        }

        public event EventHandler<DocumentEventArgs>? DocumentQueued;
        public event EventHandler<DocumentEventArgs>? DocumentInserting;
        public event EventHandler<DocumentEventArgs>? DocumentInserted;
        public event EventHandler<DocumentEventArgs>? DocumentFailed;

        public string DatabaseName { get; }

        public string ContainerName { get; }

        public ISnapshotHandle Destination { get; }

        public QueryDefinition DocumentsQuery { get; }

        public void OnDocumentInserted(DocumentEventArgs e) => DocumentInserted?.Invoke(this, e);
        public void OnDocumentInserting(DocumentEventArgs e) => DocumentInserting?.Invoke(this, e);
        public void OnDocumentQueued(DocumentEventArgs e) => DocumentQueued?.Invoke(this, e);
        public void OnDocumentFailed(DocumentEventArgs e) => DocumentFailed?.Invoke(this, e);
    }
}
