using System;
using System.Collections.Generic;
using System.Linq;
using Gruppo.Storage;

namespace Gruppo.MessageBroker
{
  public class MessageBroker : IMessageBroker
  {
    private Dictionary<string, Topic> _topics;

    public MessageBroker(GruppoSettings settings)
    {
      _topics = new Dictionary<string, Topic>();
      this._settings = settings;
    }   

    public void Consume(string topicName, string group, out GruppoMessage message)
    {
      message = null;

      Topic topic = GetTopic(topicName);

      if (topic != null)
      {
        topic.Consume(group, out message);
      }
    }


    public void CreateTopic(string topicName)
    {
      lock (_topics)
      {
        if (!_topics.ContainsKey(topicName))
        {
          var fileSystem = new HardDriveFileSystem(_settings.StorageDirectory, topicName, _settings.MaxMessagesInMessageFile);
          _topics.Add(topicName, new Topic(topicName, fileSystem));
        }
      }
    }

    public string[] GetTopicNames()
    {
      lock( _topics )
      {
        return _topics.Keys.ToArray();
      }
    }

    public TopicStatistics[] GetTopicStatistics()
    {
      lock( _topics )
      {
        return _topics.Values.Select(t => t.GetStats()).ToArray();
      }
    }

    public void Peek(string topicName, long offset, out GruppoMessage message)
    {
      message = null;
      Topic topic = GetTopic(topicName);
      if ( topic != null )
      {
        topic.Peek(offset, out message);
      }
    }

    public void Produce(string topicName, string meta, byte[] body, out long offset, out DateTime timestamp)
    {
      Topic topic = GetTopic(topicName);

      if (topic == null)
      {
        CreateTopic(topicName);
        topic = GetTopic(topicName);
      }

      {
        GruppoMessage message = new GruppoMessage()
        {
          Meta = meta,
          Body = body
        };

        topic.Produce(message, out offset, out timestamp);
      }
    }

    private Topic GetTopic(string topicName)
    {
      Topic topic;
      lock (_topics)
      {
        _topics.TryGetValue(topicName, out topic);
      }

      return topic;
    }

    public void SetOffset(string topicName, string groupName, int offset)
    {
      var topic = GetTopic(topicName);
      if ( topic != null )
      {
        topic.SetOffset(groupName, offset);
      }
    }

    private bool disposedValue = false;
    private readonly GruppoSettings _settings;

    protected virtual void Dispose(bool disposing)
    {
      if (!disposedValue)
      {
        if (disposing)
        {
          foreach(var topic in _topics)
          {
            topic.Value.Dispose();
          }

          _topics.Clear();
        }


        disposedValue = true;
      }
    }

    public void Dispose()
    {
      Dispose(true);
    }

    public void Consume(string topicName, long offset, out GruppoMessage message)
    {
      message = null;
      Topic topic = GetTopic(topicName);

      if (topic != null)
      {
        topic.Consume(offset, out message);
      }
    }
  }
}
