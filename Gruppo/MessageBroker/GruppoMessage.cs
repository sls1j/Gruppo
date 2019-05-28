using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Gruppo.MessageBroker
{
  public class GruppoMessage
  {
    public GruppoMessage()
    {
    }

    public GruppoMessage(string meta, string body, DateTime? timestamp = null, long? offset = null)
    {
      Timestamp = timestamp;
      Offset = offset;
      Meta = meta;
      Body = Encoding.UTF8.GetBytes(body);
    }

    public DateTime? Timestamp;
    public long? Offset;
    public string Meta;
    public byte[] Body;
  }
}
