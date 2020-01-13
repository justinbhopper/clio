using System;

namespace RH.Clio.Cosmos
{
    public interface IDocumentWriter
    {
        event EventHandler<DocumentEventArgs>? ThrottleWaitStarted;

        event EventHandler<DocumentEventArgs>? ThrottleWaitFinished;

        event EventHandler<DocumentEventArgs>? DocumentQueued;

        event EventHandler<DocumentEventArgs>? DocumentInserting;

        event EventHandler<DocumentEventArgs>? DocumentInserted;

        event EventHandler<DocumentEventArgs>? DocumentFailed;
    }
}
