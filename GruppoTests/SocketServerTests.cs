using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Force.Crc32;
using Gruppo.Communications;
using Gruppo.SDK.Communications;
using NUnit.Framework;

namespace GruppoTests
{
  public class SocketServerTests
  {

    [Test]
    public void TestClientStartEvent()
    {
      string expectedIp = "127.0.0.1";
      int expectedPort = 44400;
      SocketServer target = new SocketServer();
      target.Port = expectedPort;
      target.BindingIPs = new string[] { expectedIp };
      int clientStartCount = 0;

      target.RegisterClientEvents(c => clientStartCount++, null, null);
      target.Start();

      TcpClient client = new TcpClient();
      client.Connect(expectedIp, expectedPort);
      Thread.Sleep(250);
      client.Close();

      Assert.AreEqual(clientStartCount, 1);
    }

    [Test]
    public void TestClientStopEvent()
    {
      string expectedIp = "127.0.0.1";
      int expectedPort = 44401;
      SocketServer target = new SocketServer();
      target.Port = expectedPort;
      target.BindingIPs = new string[] { expectedIp };
      int clientStartCount = 0;
      int clientStopCount = 0;
      using (AutoResetEvent isStarted = new AutoResetEvent(false))
      using (var isStopped = new AutoResetEvent(false))
      {
        target.RegisterClientEvents(c =>
        {
          clientStartCount++;
          isStarted.Set();
        },
        c =>
        {
          clientStopCount++;
          isStopped.Set();
        },
        null);
        target.Start();

        TcpClient client = new TcpClient();
        client.Connect(expectedIp, expectedPort);
        isStarted.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);
        client.Close();
        isStopped.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);
        target.Stop();
      }

      Assert.AreEqual(clientStartCount, 1);
      Assert.AreEqual(clientStopCount, 1);
    }

    [Test]
    public void TestClientMessageReceiveEvent()
    {
      string expectedIp = "127.0.0.1";
      int expectedPort = 44402;
      SocketServer target = new SocketServer();
      target.Port = expectedPort;
      target.BindingIPs = new string[] { expectedIp };
      int clientStartCount = 0;
      int clientStopCount = 0;
      int messagesReceived = 0;
      string actualMeta = null;
      byte[] actualBody = null;
      long clientId = -1;
      using (AutoResetEvent isStarted = new AutoResetEvent(false))
      using (AutoResetEvent messageReceived = new AutoResetEvent(false))
      {
        target.RegisterClientEvents(c =>
        {
          clientStartCount++;
          clientId = c.Id;
          isStarted.Set();
        }
        , c => clientStopCount++
        , (c, m, bs, b) =>
        {
          messagesReceived++;
          actualMeta = m;
          using (var stream = new MemoryStream())
          {
            b.CopyTo(stream);
            actualBody = stream.ToArray();
          }
          messageReceived.Set();
        });

        target.Start();

        TcpClient client = new TcpClient();
        client.Connect(expectedIp, expectedPort);
        isStarted.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);

        string meta = "This is a test";
        byte[] metaBytes = Encoding.UTF8.GetBytes(meta);
        byte[] body = Encoding.UTF8.GetBytes("This is the body");
        var t = Crc32Algorithm.Append(0, metaBytes);
        var crc = Crc32Algorithm.Append(t, body);
        MessageHeader header = new MessageHeader()
        {
          BodySize = (ulong)body.LongLength,
          MetaSize = (ushort)metaBytes.Length,
        };

        byte[] headerBytes = header.SerializeHeader();

        using (var stream = client.GetStream())
        {
          stream.Write(headerBytes, 0, headerBytes.Length);
          stream.Write(metaBytes, 0, metaBytes.Length);
          stream.Write(body, 0, body.Length);
        }

        messageReceived.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);

        target.Stop();

        Assert.AreEqual(1, clientStartCount);
        Assert.AreEqual(1, messagesReceived);
        Assert.AreEqual(meta, actualMeta);
        Assert.AreEqual(body, actualBody);
        Assert.AreEqual(1, clientStopCount);
      }
    }

    [Test]
    public void TestClientMessageSendEvent()
    {
      string expectedIp = "127.0.0.1";
      int expectedPort = 44403;
      SocketServer target = new SocketServer();
      target.Port = expectedPort;
      target.BindingIPs = new string[] { expectedIp };
      int clientStartCount = 0;
      int clientStopCount = 0;
      long clientId = -1;
      using (var isStarted = new AutoResetEvent(false))
      using (var isStopped = new AutoResetEvent(false))
      {

        target.RegisterClientEvents(c =>
        {
          clientStartCount++;
          clientId = c.Id;
          isStarted.Set();
        }
        , c =>
        {
          clientStopCount++;
          isStopped.Set();
        }
        , (c, m, bs, b) => { }
        );
        target.Start();

        TcpClient client = new TcpClient();
        client.Connect(expectedIp, expectedPort);

        isStarted.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);

        string expectedMeta = "This is a test";
        string expectedBody = "This is a test body";
        byte[] body = Encoding.UTF8.GetBytes(expectedBody);
        using (MemoryStream bodyStream = new MemoryStream(body))
        {
          target.SendMessage(clientId, expectedMeta, bodyStream);
        }

        string actualMeta = null;
        string actualBody = null;
        using (var stream = client.GetStream())
        {
          byte[] headerBytes = new byte[MessageHeader.BinarySize];
          // read header
          stream.Read(headerBytes, 0, headerBytes.Length);
          var header = MessageHeader.DeserializeHeader(headerBytes);

          // read the meta data and convert to string
          byte[] metaBytes = new byte[header.MetaSize];
          stream.Read(metaBytes, 0, metaBytes.Length);

          actualMeta = Encoding.UTF8.GetString(metaBytes);
          byte[] bodyBytes = new byte[header.BodySize];
          stream.Read(bodyBytes, 0, bodyBytes.Length);

          actualBody = Encoding.UTF8.GetString(bodyBytes);
        }

        client.Close();
        isStopped.WaitOne(Debugger.IsAttached ? Timeout.Infinite : 1500);

        target.Stop();
        
        Assert.AreEqual(clientStartCount, 1);
        Assert.AreEqual(clientStopCount, 1);

        Assert.AreEqual(expectedMeta, actualMeta);
        Assert.AreEqual(expectedBody, actualBody);
      };
    }

  }
}
