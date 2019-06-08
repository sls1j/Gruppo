using System;

namespace Gruppo.MessageBroker
{
  public interface ITopic : IDisposable
  {
    string Name { get; }
    bool Produce(GruppoMessage message, out long offset, out DateTime timestamp);
    
    void Consume(string group, out GruppoMessage message);

    void Consume(long offset, out GruppoMessage message);

    void Peek(long offset, out GruppoMessage message);

    TopicStatistics GetStats();
  }
}
