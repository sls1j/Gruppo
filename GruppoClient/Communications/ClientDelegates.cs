using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Gruppo.SDK.Communications

{
  public delegate void ClientStartedDelegate(ISocketClient client);
  public delegate void ClientStoppedDelegate(ISocketClient client);
  public delegate void ClientMessageReceived(ISocketClient client, string meta, ulong bodySize, Stream body);
}
