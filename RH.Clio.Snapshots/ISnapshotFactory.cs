namespace RH.Clio.Snapshots
{
    public interface ISnapshotFactory
    {
        ISnapshotReader CreateReader();

        ISnapshotHandle CreateWriter();
    }
}
