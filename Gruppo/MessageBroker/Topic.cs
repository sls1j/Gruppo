using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Gruppo.Client.Utilities;
using Gruppo.SDK.Exceptions;
using Gruppo.Storage;
using GruppoClient.Utilities;

namespace Gruppo.MessageBroker
{
  public class Topic : ITopic
  {
    private ExecutionGuard Guard;
    private IFileSystem FileSystem;
    private long ProduceOffset;
    private BinaryWriter ProduceIndexWriter;
    private BinaryWriter ProduceMessageWriter;
    private Dictionary<string, Group> GroupConsumers;
    private GruppoSettings Settings;


    public Topic(string topicName, GruppoSettings settings, Func<string, string, IFileSystem> fileSystemFactory)
    {
      if (!Regex.IsMatch(topicName, @"^[\d\-\.a-z]*$"))
        throw new InvalidTopicNameException($"'{topicName} is not valid.");

      Settings = settings.NotNull(nameof(settings));

      Guard = new ExecutionGuard();

      Name = topicName.NotNullOrEmpty(nameof(topicName));
      FileSystem = fileSystemFactory.NotNull(nameof(fileSystemFactory))(Settings.StorageDirectory, topicName);
      GroupConsumers = new Dictionary<string, Group>();
      LoadGroupIndexs();

      // find the product offset on startup
      ProduceOffset = 0;
      using (var reader = FileSystem.OpenIndexReader())
      {
        if (reader.BaseStream.Length >= sizeof(long))
        {
          reader.BaseStream.Seek(sizeof(long), SeekOrigin.End);
          ProduceOffset = reader.ReadInt64();
        }
      }

      ProduceIndexWriter = FileSystem.OpenIndexWriter();
      ProduceIndexWriter.BaseStream.Seek(0, SeekOrigin.End);

      ProduceMessageWriter = FileSystem.OpenMessageFileWriter(ProduceOffset);
      ProduceMessageWriter.BaseStream.Seek(0, SeekOrigin.End);
    }

    private void LoadGroupIndexs()
    {
      foreach (var groupName in FileSystem.EnumerateGroupNames())
      {
        Group g = CreateGroup(groupName);

        GroupConsumers.Add(groupName, g);
      }
    }

    private Group CreateGroup(string groupName)
    {
      Group g = new Group();
      g.Name = groupName;
      using (var reader = FileSystem.OpenGroupIndexFileReader(groupName))
      {
        g.GroupIndexWriter = FileSystem.OpenGroupIndexFileWriter(g.Name);
        g.Offset = (reader.BaseStream.Length < sizeof(Int64)) ? 0 : reader.ReadInt64();
        g.IndexReader = FileSystem.OpenIndexReader();
        g.MessageReader = FileSystem.OpenMessageFileReader(g.Offset);

        // read the position in the message file
        long indexPosition = g.Offset * sizeof(Int64);
        g.IndexReader.BaseStream.Position = indexPosition;
        long messagePosition = (reader.BaseStream.Length < sizeof(Int64)) ? 0 : g.IndexReader.ReadInt64();
        g.MessageReader.BaseStream.Position = messagePosition;

        // reset the index reader
        g.IndexReader.BaseStream.Position = indexPosition;
      }

      return g;
    }

    public string Name { get; private set; }

    public void Consume(string groupName, out GruppoMessage message)
    {
      if (Guard.EnterExecute())
      {
        try
        {
          Group group;
          lock (GroupConsumers)
          {
            if (!GroupConsumers.TryGetValue(groupName, out group))
            {
              group = CreateGroup(groupName);
              GroupConsumers.Add(groupName, group);
            }
          }

          lock (group)
          {
            // is there a message to read?
            if (group.IndexReader.BaseStream.Position < group.IndexReader.BaseStream.Length)
            {
              // advance the index reader -- this is an atomic way of knowing if we have finished reading a message
              // writing the index takes one step, writing the message takes several steps and we shouldn't try to read
              // a message until it's completely written.  The index is the last thing written so wait on it.
              long messagePosition = group.IndexReader.ReadInt64();

              // if the offset is zero then a new message file has been reached get it
              if (messagePosition == 0)
              {
                group.MessageReader.Dispose();
                group.MessageReader = FileSystem.OpenMessageFileReader(group.Offset);
              }

              // read the message
              message = ReadMessage(group.MessageReader, group.Offset, true);
              group.Offset++;

              // write group index out
              group.GroupIndexWriter.BaseStream.Position = 0;
              group.GroupIndexWriter.Write(group.Offset);
            }
            else
            {
              message = null;
            }
          }
        }
        finally
        {
          Guard.ExitExecute();
        }
      }
      else
        message = null;
    }

    public void Consume(long offset, out GruppoMessage message)
    {
      if (Guard.EnterExecute())
      {
        try
        {
          using (var index = FileSystem.OpenIndexReader())
          using (var messageReader = FileSystem.OpenMessageFileReader(offset))
          {
            index.BaseStream.Seek((offset) * sizeof(Int64), SeekOrigin.Begin);
            long beginOffset = index.ReadInt64();
            messageReader.BaseStream.Seek(beginOffset, SeekOrigin.Begin);
            message = ReadMessage(messageReader, offset, true);
          }
        }
        finally
        {
          Guard.ExitExecute();
        }
      }
      else
        message = null;
    }

    private static GruppoMessage ReadMessage(BinaryReader messageReader, long offset, bool readBody)
    {
      GruppoMessage message = new GruppoMessage();
      message.Offset = offset;
      message.Timestamp = new DateTime(messageReader.ReadInt64());
      message.Meta = messageReader.ReadString();
      int length = messageReader.ReadInt32();
      if (readBody)
        message.Body = messageReader.ReadBytes(length);

      return message;
    }

    public void Dispose()
    {
      if (ProduceIndexWriter != null)
      {
        ProduceIndexWriter.Dispose();
        ProduceIndexWriter = null;
      }

      if (ProduceMessageWriter != null)
      {
        ProduceMessageWriter.Dispose();
        ProduceIndexWriter = null;
      }
    }

    public TopicStatistics GetStats()
    {
      lock (this)
      {
        return new TopicStatistics()
        {
          StorageDirectory = FileSystem.TopicDirectory,
          MessageCount = ProduceOffset
        };
      }
    }

    public void Peek(long offset, out GruppoMessage message)
    {
      if (Guard.EnterExecute())
      {
        try
        {
          using (var index = FileSystem.OpenIndexReader())
          using (var messageReader = FileSystem.OpenMessageFileReader(offset))
          {
            long position = offset * sizeof(long);
            if (position > index.BaseStream.Length)
            {
              message = null;
              return;
            }

            index.BaseStream.Position = position;
            long messagePosition = index.ReadInt64();
            messageReader.BaseStream.Position = messagePosition;
            message = ReadMessage(messageReader, offset, false);
          }
        }
        finally
        {
          Guard.ExitExecute();
        }
      }
      else
        message = null;
    }

    public bool Produce(GruppoMessage message, out long offset, out DateTime timestamp)
    {
      if (Guard.EnterExecute())
      {
        try
        {
          lock (this)
          {
            // write out the message to the message stream
            timestamp = DateTime.UtcNow;
            long beginOffset = ProduceMessageWriter.BaseStream.Position;
            ProduceMessageWriter.Write(timestamp.Ticks);
            ProduceMessageWriter.Write(message.Meta ?? string.Empty);
            if (message.Body == null)
              ProduceMessageWriter.Write((int)0);
            else
            {
              ProduceMessageWriter.Write(message.Body.Length);
              ProduceMessageWriter.Write(message.Body);
            }
            ProduceMessageWriter.Flush();

            offset = ProduceOffset++;

            // write message start position to the index
            ProduceIndexWriter.Write(beginOffset);
            ProduceIndexWriter.Flush();
          }

          // check to see if the file needs to be rolled
          if (ProduceOffset % Settings.MaxMessagesInMessageFile == 0)
          {
            CreateProduceWriters();
          }

          return true;
        }
        finally
        {
          Guard.ExitExecute();
        }
      }
      else
      {
        offset = -1;
        timestamp = DateTime.MinValue;
        return false;
      }
    }


    private void CreateProduceWriters()
    {
      ProduceMessageWriter.Dispose();
      ProduceMessageWriter = FileSystem.OpenMessageFileWriter(ProduceOffset);
    }

    private class Group
    {
      public string Name;
      public long Offset;
      public BinaryReader IndexReader;
      public BinaryReader MessageReader;
      public BinaryWriter GroupIndexWriter;
    }
  }
}
