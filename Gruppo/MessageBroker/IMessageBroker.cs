using System;
using System.IO;

namespace Gruppo.MessageBroker
{
  public interface IMessageBroker: IDisposable
  {
    void CreateTopic(string topic);
    void Produce(string topicName, string meta, byte[] body, out long offset, out DateTime timestamp);
    void Consume(string topicName, long offset, out GruppoMessage message);
    void Consume(string topicName, string groupName, out GruppoMessage message);
    void Peek(string topicName, long offset, out GruppoMessage message);
    void SetOffset(string topicName, string groupName, int offset);
    string[] GetTopicNames();
    TopicStatistics[] GetTopicStatistics();
  }
}
