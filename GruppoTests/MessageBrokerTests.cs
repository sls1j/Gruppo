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
      broker.Start();
      string expectedTopicA = "test_topic_a";
      string expectedTopicB = "test_topic_a";
      broker.CreateTopic(expectedTopicA);
      var topicNames = broker.GetTopicNames();
      Assert.AreEqual(1, topicNames.Length);
      Assert.AreEqual(expectedTopicA, topicNames[0]);
      broker.CreateTopic(expectedTopicB);
      topicNames = broker.GetTopicNames();
      Assert.AreEqual(2, topicNames.Length);
      Assert.IsTrue(topicNames.Any(t => t == expectedTopicA));
      Assert.IsTrue(topicNames.All(t => t == expectedTopicB));
      broker.Stop();
    }

    [Test]
    public void ProduceMessageTest()
    {
      string[] expectedTopics = new string[] { "a", "b", "c" };
      MessageBroker broker = MakeBroker();
      broker.Start();
      broker.CreateTopic(expectedTopics[0]);

      int predictedOffset = 0;
      const int expectedMessageCount = 10;
      foreach(string topicName in expectedTopics)
      {
        for(int i=0; i < expectedMessageCount; i++)
        {
          using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes($"Message body {i}")))
          {
            int offset;
            broker.ProduceMessage(topicName, $"message {i}", ms, out offset);
            Assert.AreEqual(predictedOffset, offset);
            predictedOffset++;
          }
        }
      }

      var stats = broker.GetTopicStatistics();
      foreach(var stat in stats)
      {
        Assert.IsTrue(expectedTopics.Any(tn => tn == stat.Name));
        Assert.AreEqual(expectedMessageCount, stat.MessageCount);
      }
    }

    [Test]
    public void PeekMessageTest()
    {
      Assert.Inconclusive();
    }

    [Test]
    public void ConsumeMessageTest()
    {
      Assert.Inconclusive();
    }

    [Test]
    public void ConsumeMessageAtTest()
    {
      Assert.Inconclusive();
    }

    [Test]
    public void SetOffsetTest()
    {
      Assert.Inconclusive();
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
