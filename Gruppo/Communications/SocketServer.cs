using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Gruppo.Client.Utilities;
using Gruppo.SDK.Communications;

namespace Gruppo.Communications
{
  public class SocketServer : ISocketServer
  {
    private ExecutionGuard Guard;
    private Dictionary<long, SocketClient> Clients;

    private Socket[] Servers;
    private ClientStartedDelegate OnStart;
    private ClientStoppedDelegate OnStop;
    private ClientMessageReceived OnMessage;

    public SocketServer()
    {
      Guard = new ExecutionGuard();
      Clients = new Dictionary<long, SocketClient>();
    }
    public string[] BindingIPs { get; set; }
    public int Port { get; set; }

    public ISocketClient[] GetClients()
    {
      lock (Clients)
      {
        return Clients.Values.ToArray();
      }
    }

    public void RegisterClientEvents(ClientStartedDelegate onStarted, ClientStoppedDelegate onStopped, ClientMessageReceived onMessage)
    {
      OnStart = onStarted;
      OnStop = onStopped;
      OnMessage = onMessage;
    }

    public void SendMessage(long ClientId, string meta, Stream body)
    {
      lock (Clients)
      {
        var client = Clients[ClientId];
        client.Send(meta, body);
      }
    }

    public void Start()
    {
      // Start the services
      if (BindingIPs == null || BindingIPs.Length == 0)
      {
        Servers = new Socket[1];
        Socket socket = CreateSocket(new IPEndPoint(IPAddress.Any, Port));
        Servers[0] = socket;
      }
      else
      {
        Servers = new Socket[BindingIPs.Length];
        for (int i = 0; i < BindingIPs.Length; i++)
        {
          Servers[i] = CreateSocket(new IPEndPoint(IPAddress.Parse(BindingIPs[i]), Port));
        }
      }
    }

    private Socket CreateSocket(IPEndPoint endPoint)
    {
      var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(endPoint);
      socket.Listen(5);
      socket.BeginAccept(OnAccept, socket);
      return socket;
    }

    private void OnAccept(IAsyncResult ar)
    {
      if (Guard.EnterExecute())
      {
        Socket socket = ar.AsyncState as Socket;
        Socket acceptedSocket = socket.EndAccept(ar);

        try
        {
          // set socket to accept another connection
          socket.BeginAccept(OnAccept, socket);
          var serverClient = new SocketClient(acceptedSocket);
          lock (Clients)
          {
            Clients[serverClient.Id] = serverClient;
            serverClient.Start( DoStart, DoClientMessage, DoClientStopped);
          }
        }
        finally
        {
          Guard.ExitExecute();
        }
      }
    }

    private void DoStart(ISocketClient serverClient)
    {
      try
      {
        OnStart(serverClient);
      }
      catch (Exception)
      {

      }
    }

    private void DoClientStopped(ISocketClient client)
    {
      Guard.Execute(() =>
      {
        OnStop(client);
        lock (Clients)
        {
          Clients.Remove(client.Id);
        }
        (client as SocketClient).Dispose();
      });
    }

    private void DoClientMessage(ISocketClient client, string meta, ulong bodySize, Stream stream)
    {
      Guard.Execute(() =>
     {
       if (OnMessage != null)
       {
         try
         {
           OnMessage(client, meta, bodySize, stream);
         }
         catch (Exception)
         {

         }
       }
     });
    }

    public void Stop()
    {
      if (Guard.DisableExecute())
      {

        for (int i = 0; i < Servers.Length; i++)
        {
          Servers[i].Close();
        }

        lock (Clients)
        {
          foreach (var kv in Clients)
          {
            kv.Value.Dispose();
          }

          Clients.Clear();
        }
      }
      else
      {
        throw new InvalidOperationException("Can only call Stop once.");
      }
    }
  }
}
