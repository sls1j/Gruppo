using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Gruppo.SDK.Exceptions
{
  public class InvalidTopicNameException : Exception
  {
    public InvalidTopicNameException()
    {

    }

    public InvalidTopicNameException(string message)
      :base(message)
    {

    }

    public InvalidTopicNameException(string message, Exception innerException)
      :base(message, innerException)
    {

    }

    public InvalidTopicNameException(SerializationInfo info, StreamingContext context)
      :base(info,context)
    {

    }
  }
}
