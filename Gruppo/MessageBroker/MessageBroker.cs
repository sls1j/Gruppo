using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Gruppo.MessageBroker
{
  public class MessageBroker : IMessageBroker
  {
    public MessageBroker(GruppoSettings settings)
    {

    }

    public void Start()
    {

    }

    public void Stop()
    {

    }    

    public void ConsumeMessage(string group, string topic, GruppoMessage message)
    {
      throw new NotImplementedException();
    }

    public void CreateTopic(string topic)
    {
      throw new NotImplementedException();
    }

    public string[] GetTopicNames()
    {
      throw new NotImplementedException();
    }

    public TopicStatistics[] GetTopicStatistics()
    {
      throw new NotImplementedException();
    }

    public void PeekMessage(string group, string topic, GruppoMessage message)
    {
      throw new NotImplementedException();
    }

    public void ProduceMessage(string topic, string meta, Stream body, out int offset)
    {
      throw new NotImplementedException();
    }

    public void SetOffset(string group, int offset)
    {
      throw new NotImplementedException();
    }
    
    private bool disposedValue = false; 

    protected virtual void Dispose(bool disposing)
    {
      if (!disposedValue)
      {
        if (disposing)
        {
          Stop();
        }


        disposedValue = true;
      }
    }

    public void Dispose()
    {
      Dispose(true);
    }
  }
}
