using System;
using System.IO;

namespace Gruppo.MessageBroker
{
  public interface IMessageBroker: IDisposable
  {
    void Start();
    void Stop();
    void CreateTopic(string topic);
    void ProduceMessage(string topic, string meta, Stream body, out int offset);
    void ConsumeMessageAt(int offset, string topic, out GruppoMessage message);
    void ConsumeMessage(string group, string topic, out GruppoMessage message);
    void PeekMessage(string group, string topic, out GruppoMessage message);
    void SetOffset(string group, int offset);
    string[] GetTopicNames();
    TopicStatistics[] GetTopicStatistics();
  }
}
