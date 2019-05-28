using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Gruppo.MessageBroker
{
  interface IMessageFile
  {
    void Write(string meta, Stream body, out long offset);
    GruppoMessage Read(long offset);
  }
}
