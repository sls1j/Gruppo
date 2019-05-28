using System.IO;
using Gruppo.SDK.Communications;

namespace Gruppo.Communications
{
  public interface ISocketServer
  {
    string[] BindingIPs { get; set; }
    int Port { get; set; }
    void Start();
    void Stop();
    ISocketClient[] GetClients();
    void RegisterClientEvents(ClientStartedDelegate onStarted, ClientStoppedDelegate onStopped, ClientMessageReceived onMessage);
    void SendMessage(long ClientId, string meta, Stream body);
  }
}
