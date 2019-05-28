using System.Net;

namespace Gruppo.SDK.Communications
{
  public interface ISocketClient
  {
    long Id { get; }
    EndPoint RemoteEndPoint { get; }
  }
}