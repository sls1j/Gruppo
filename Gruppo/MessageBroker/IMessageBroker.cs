using System.IO;

namespace Gruppo.MessageBroker
{
  public interface IMessageBroker
  {
    void CreateTopic(string topic);
    void ProduceMessage(string topic, string meta, Stream body, out int offset);
    void ConsumeMessage(string group, string topic, GruppoMessage message);
    void SetConsumeOffset(string group, int offset);
    void PeekMessage(string group, string topic, GruppoMessage message);
    void SetOffset(string group, int offset);
    string[] GetTopicNames();
    TopicStatistics[] GetTopicStatistics();
  }
}
