using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Gruppo;
using Gruppo.MessageBroker;
using NUnit.Framework;

namespace GruppoTests
{
  public class MessageBrokerTests
  {
    [Test]
    public void ConstructorTest()
    {
      GruppoSettings settings = new GruppoSettings();
      MessageBroker broker = new MessageBroker(settings);
    }

    [Test]
    public void CreateTopicTest()
    {
      MessageBroker broker = MakeBroker();
      string expectedTopicA = "test_topic_a";
      string expectedTopicB = "test_topic_b";
      broker.CreateTopic(expectedTopicA);
      var topicNames = broker.GetTopicNames();
      Assert.AreEqual(1, topicNames.Length);
      Assert.AreEqual(expectedTopicA, topicNames[0]);
      broker.CreateTopic(expectedTopicB);
      topicNames = broker.GetTopicNames();
      Assert.AreEqual(2, topicNames.Length);
      Assert.IsTrue(topicNames.Any(t => t == expectedTopicA));
      Assert.IsTrue(topicNames.Any(t => t == expectedTopicB));
    }

    [Test]
    public void ProduceMessageTest()
    {
      string[] expectedTopics = new string[] { "a", "b", "c" };
      MessageBroker broker = MakeBroker();
      broker.CreateTopic(expectedTopics[0]);

      const int expectedMessageCount = 10;
      foreach (string topicName in expectedTopics)
      {
        int predictedOffset = 0;
        for (int i = 0; i < expectedMessageCount; i++)
        {
          long offset;
          DateTime timestamp;
          broker.Produce(topicName, $"message {i}", Encoding.UTF8.GetBytes($"Message body {i}"), out offset, out timestamp);
          Assert.AreEqual(predictedOffset, offset);
          predictedOffset++;
        }
      }

      var stats = broker.GetTopicStatistics();
      foreach (var stat in stats)
      {
        Assert.IsTrue(expectedTopics.Any(tn => tn == stat.Name));
        Assert.AreEqual(expectedMessageCount, stat.MessageCount);
      }
    }

    [Test]
    public void PeekMessageTest()
    {
      string expectedTopic = "topic";
      string expectedMeta = "This is a meta";
      string expectedBody = "This is a body";
      MessageBroker broker = MakeBroker();
      long offset;
      DateTime timestamp;
      broker.Produce(expectedTopic, expectedMeta, Encoding.UTF8.GetBytes(expectedBody), out offset, out timestamp);
      GruppoMessage message;
      broker.Peek(expectedTopic, 0, out message);
      Assert.IsNotNull(message);
      Assert.IsNull(message.Body);
      Assert.AreEqual(expectedMeta, message.Meta);
      Assert.AreEqual(offset, message.Offset);
    }

    [Test]
    public void ConsumeMessageTest()
    {
      string[] expectedTopics = new string[] { "a", "b", "c" };
      MessageBroker broker = MakeBroker();
      broker.CreateTopic(expectedTopics[0]);

      const int expectedMessageCount = 10;
      foreach (string topicName in expectedTopics)
      {
        for (int i = 0; i < expectedMessageCount; i++)
        {
          long offset;
          DateTime timestamp;
          broker.Produce(topicName, $"message {i}", Encoding.UTF8.GetBytes($"Message body {i}"), out offset, out timestamp);
        }
      }

      List<GruppoMessage> messages = new List<GruppoMessage>();
      foreach (string topic in expectedTopics)
      {
        for (int i = 0; i < expectedMessageCount + 1; i++)
        {
          GruppoMessage message;
          broker.Consume(topic, "group_1", out message);
          if (i == expectedMessageCount)
          {
            Assert.IsNull(message);
          }
          else
          {
            Assert.IsNotNull(message);
            messages.Add(message);
            Assert.AreEqual(i, message.Offset);
          }
        }
      }
    }

    [Test]
    public void ConsumeMessageAtTest()
    {
      string[] expectedTopics = new string[] { "a", "b", "c" };
      MessageBroker broker = MakeBroker();
      broker.CreateTopic(expectedTopics[0]);

      const int expectedMessageCount = 10;
      foreach (string topicName in expectedTopics)
      {
        for (int i = 0; i < expectedMessageCount; i++)
        {
          long offset;
          DateTime timestamp;
          broker.Produce(topicName, $"message {i}", Encoding.UTF8.GetBytes($"Message body {i}"), out offset, out timestamp);
        }
      }

      GruppoMessage message;
      broker.Consume(expectedTopics[0], expectedMessageCount - 5, out message);

      Assert.IsNotNull(message);
      Assert.AreEqual(5, message.Offset);
    }

    [Test]
    public void SetOffsetTest()
    {
      string topicName = "a";
      MessageBroker broker = MakeBroker();

      const int expectedMessageCount = 10;
      for (int i = 0; i < expectedMessageCount; i++)
      {
        long offset;
        DateTime timestamp;
        broker.Produce(topicName, $"message {i}", Encoding.UTF8.GetBytes($"Message body {i}"), out offset, out timestamp);
      }

      string groupName = "group_1";

      GruppoMessage message;
      for (int i = 0; i < expectedMessageCount; i++)
      {
        broker.Consume(groupName, topicName, out message);
      }

      broker.SetOffset(topicName, groupName, 1);
      broker.Consume(topicName, groupName, out message);

      Assert.IsNotNull(message);
      Assert.AreEqual(1, message.Offset);
    }

    private MessageBroker MakeBroker()
    {
      var context = TestContext.CurrentContext;
      var storagePath = Path.Combine(context.WorkDirectory, context.Test.Name, context.Test.ID, "topics");

      if (Directory.Exists(storagePath))
        Directory.Delete(storagePath, true);

      var settings = new GruppoSettings()
      {
        MaxMessagesInMessageFile = 100,
        StorageDirectory = storagePath
      };


      return new MessageBroker(settings);
    }
  }
}
