namespace RH.Clio.Commands
{
    public interface IRequest { }

    public interface IRequest<out TResponse> { }
}
