using System;

namespace RH.Clio.Cosmos
{
    public interface IDocumentWriter
    {
        event EventHandler? ThrottleWaitStarted;

        event EventHandler? ThrottleWaitFinished;

        event EventHandler? DocumentQueued;

        event EventHandler? DocumentInserting;

        event EventHandler? DocumentInserted;
    }
}
