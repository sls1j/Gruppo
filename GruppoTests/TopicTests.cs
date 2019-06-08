using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Gruppo;
using Gruppo.MessageBroker;
using Gruppo.SDK.Exceptions;
using Gruppo.Storage;
using NUnit.Framework;

namespace GruppoTests
{
  public class TopicTests
  {

    [Test]
    public void ConstructorTest()
    {
      string expectedTopicName = "a";
      var context = TestContext.CurrentContext;
      var expectedStoragePath = Path.Combine(context.WorkDirectory, context.Test.Name, context.Test.ID, "topics");

      if (Directory.Exists(expectedStoragePath))
        Directory.Delete(expectedStoragePath, true);

      using (Topic topicA = new Topic(
        expectedTopicName, new HardDriveFileSystem(expectedStoragePath, expectedTopicName, 100)))
      {

        TopicStatistics stats = topicA.GetStats();

        Assert.AreEqual(expectedTopicName, topicA.Name);
        Assert.NotNull(stats);
        Assert.AreEqual(Path.Combine(expectedStoragePath, "topics", expectedTopicName), stats.StorageDirectory);
      }
    }

    [Test]
    public void TopicNameValidationTests()
    {
      string[] validNames = new string[] { "word", "word-again", "123-topic", "topic-george", "topic.stuff" };
      string[] invalidNames = new string[] { "Test", "*junk", "stuff/more", "stuff\\more", "weird\nstuff", "don't", "do this", "OrThis" };

      // check topic validation
      foreach (string validTopic in validNames)
      {
        try
        {
          var t = BuildTopic(validTopic);
          t.Dispose();
        }
        catch (Exception)
        {
          Assert.Fail();
        }
      }

      // check that these invlaid name don't work
      foreach (string invalidName in invalidNames)
      {
        try
        {
          var t = BuildTopic(invalidName);
          Assert.Fail();
        }
        catch (InvalidTopicNameException)
        {
        }
      }
    }

    [Test]
    public void ProduceConsumeMessageTest()
    {
      Topic topic = BuildTopic("a");
      GruppoMessage expectedMessage1 = new GruppoMessage("This is meta", "This is body");
      long actualOffset = 0;
      DateTime actualTimestamp;

      topic.Produce(expectedMessage1, out actualOffset, out actualTimestamp);

      GruppoMessage actualMessage;
      topic.Consume(actualOffset, out actualMessage);

      Assert.AreEqual(expectedMessage1.Meta, actualMessage.Meta);
      Assert.AreEqual(ByteArrayToString(expectedMessage1.Body), ByteArrayToString(actualMessage.Body));
      Assert.AreEqual(actualOffset, actualMessage.Offset.Value);
      Assert.AreEqual(actualTimestamp, actualMessage.Timestamp.Value);
    }

    [Test]
    public void ProduceManyConsumeGroupMessageTest()
    {
      Topic topic = BuildTopic("a");
      GruppoMessage expectedMessage1 = new GruppoMessage("This is meta", "This is body");
      long actualOffset = 0;
      DateTime actualTimestamp;

      List<ManualResetEvent> events = new List<ManualResetEvent>();
      for (int j = 0; j < 5; j++)
      {
        var localEvent = new ManualResetEvent(false);
        events.Add(localEvent);
        System.Threading.ThreadPool.QueueUserWorkItem(o =>
       {
         var evt = o as ManualResetEvent;
         for (int i = 0; i < 5; i++)
         {
           topic.Produce(expectedMessage1, out actualOffset, out actualTimestamp);
           Console.WriteLine($"Produced: {actualOffset} {actualTimestamp}");
         }
         evt.Set();
       }, localEvent);
      }

      foreach(var e in events)
      {
        e.WaitOne();
        e.Dispose();
      }

      GruppoMessage actualMessage;

      System.Threading.Thread.Sleep(1000);

      for (int i = 0; i < 25; i++)
      {
        topic.Consume("group_1", out actualMessage);

        Assert.NotNull(actualMessage);
        Assert.AreEqual(expectedMessage1.Meta, actualMessage.Meta);
        Assert.AreEqual(ByteArrayToString(expectedMessage1.Body), ByteArrayToString(actualMessage.Body));
      }
    }

    private string ByteArrayToString(byte[] arr)
    {
      return Encoding.UTF8.GetString(arr);
    }

    private Topic BuildTopic(string topicName)
    {
      var context = TestContext.CurrentContext;
      var storagePath = Path.Combine(context.WorkDirectory, context.Test.Name, context.Test.ID, "topics");

      if (Directory.Exists(storagePath))
        Directory.Delete(storagePath, true);

      return new Topic(topicName, new HardDriveFileSystem(storagePath, topicName, 100));
    }


  }
}
