 using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Gruppo.SDK.Communications
{
  public class SocketClient : ISocketClient, IDisposable
  {
    private static long NextId = 0;
    private ClientMessageReceived ClientMessageReceived;
    private ClientStoppedDelegate ClientStopped;
    private volatile bool Running;
    private Socket Socket;
    private NetworkStream Stream;

    public SocketClient(Socket socket)
    {
      Socket = socket;
      CommonConstructor();
    }

    public SocketClient(string host, int port)
    {
      Socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
      Socket.Connect(host, port);
      CommonConstructor();
    }

    public ClientStartedDelegate ClientStarted { get; private set; }
    public long Id { get; private set; }

    public EndPoint RemoteEndPoint { get { return Socket.RemoteEndPoint; } }

    /// <inheritdoc />
    public void Dispose()
    {
      Running = false;
      if (Socket != null)
      {
        Socket.Close();
        Socket = null;
      }

      GC.SuppressFinalize(this);
    }

    public void Send(string meta, Stream body)
    {
      MessageHeader header = new MessageHeader();
      byte[] metaBytes = Encoding.UTF8.GetBytes(meta);
      header.MetaSize = (ushort)metaBytes.Length;
      header.BodySize = (ulong)body.Length;
      byte[] headerBytes = header.SerializeHeader();
      lock (Stream)
      {
        Stream.Write(headerBytes, 0, headerBytes.Length);
        Stream.Write(metaBytes, 0, metaBytes.Length);
        byte[] buffer = new byte[Math.Min(header.BodySize, 4096)];
        while (true)
        {
          int bytesRead = body.Read(buffer, 0, buffer.Length);
          if (bytesRead == 0)
          {
            break;
          }

          Stream.Write(buffer, 0, bytesRead);
        }
      }
    }

    /// <summary>
    /// Begins reading from the socket
    /// </summary>
    /// <param name="clientMessageReceived"></param>
    /// <param name="clientStopped"></param>
    public void Start(ClientStartedDelegate clientStarted, ClientMessageReceived clientMessageReceived, ClientStoppedDelegate clientStopped)
    {
      ClientStarted = clientStarted;
      ClientMessageReceived = clientMessageReceived;
      ClientStopped = clientStopped;
      Running = true;
      ThreadPool.QueueUserWorkItem(o => DoReadState());
    }

    private void CommonConstructor()
    {
      Stream = new NetworkStream(Socket, false);
      Id = Interlocked.Increment(ref NextId);
    }
    /// <summary>
    /// The Logic for reading from the socket
    /// </summary>
    private void DoReadState()
    {
      try
      {
        try
        {
          ClientStarted(this);
        }
        catch (Exception)
        {


        }
        while (Running)
        {
          byte[] receiveBuffer = new byte[1024 * 128];
          try
          {
            // read header
            int bytesReceived = Stream.Read(receiveBuffer, 0, MessageHeader.BinarySize);
            if (bytesReceived == 0)
            {
              break;
            }

            var header = MessageHeader.DeserializeHeader(receiveBuffer);
            // read the meta data and convert to string
            bytesReceived = Stream.Read(receiveBuffer, 0, header.MetaSize);
            string meta = Encoding.UTF8.GetString(receiveBuffer, 0, bytesReceived);

            // create a stream to continue reading the message
            // push message out to event handler
            try
            {
              ClientMessageReceived(this, meta, header.BodySize, Stream);
            }
            catch (Exception)
            {

            }
          }
          catch(Exception)
          {
            break;
          }
        }
      }
      finally
      {
        try
        {
          ClientStopped(this);
        }
        catch (Exception)
        {
        }
      }
    }
  }
}
