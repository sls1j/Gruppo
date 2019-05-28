using System;
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

    private GruppoSettings Settings;


    public Topic(string topicName, GruppoSettings settings, Func<string, string, IFileSystem> fileSystemFactory)
    {
      if (!Regex.IsMatch(topicName, @"^[\d\-\.a-z]*$"))
        throw new InvalidTopicNameException($"'{topicName} is not valid.");

      Settings = settings.NotNull(nameof(settings));

      Guard = new ExecutionGuard();

      Name = topicName.NotNullOrEmpty(nameof(topicName));
      FileSystem = fileSystemFactory.NotNull(nameof(fileSystemFactory))(Settings.StorageDirectory, topicName);

      // find the product offset on startup
      ProduceOffset = 0;
      int biggestId = 1;
      foreach (var indexFilePath in FileSystem.EnumerateFiles(FileType.Index))
      {
        var match = Regex.Match(indexFilePath, @"^index_(\d*)\.bin$");

        if (match.Success)
        {
          int id = int.Parse(match.Groups[1].Value);
          if (id > biggestId)
          {
            biggestId = id;
          }
        }
      }

      ProduceIndexWriter = FileSystem.OpenFileWriter(FileType.Index, biggestId);
      ProduceIndexWriter.BaseStream.Seek(0, SeekOrigin.End);
      ProduceOffset = ProduceIndexWriter.BaseStream.Length / sizeof(Int64) + (biggestId - 1) * Settings.MaxMessagesInMessageFile;

      ProduceMessageWriter = FileSystem.OpenFileWriter(FileType.Message, biggestId);
      ProduceMessageWriter.BaseStream.Seek(0, SeekOrigin.End);
    }

    public string Name { get; private set; }

    public void Consume(string group, out GruppoMessage message)
    {
      throw new NotImplementedException();
    }

    public void Consume(long offset, out GruppoMessage message)
    {
      if (Guard.EnterExecute())
      {
        try
        {
          int id = (int)(offset / Settings.MaxMessagesInMessageFile) + 1;
          long localBegin = (offset - (id - 1) * Settings.MaxMessagesInMessageFile) * sizeof(Int64);
          using (var index = FileSystem.OpenFileReader(FileType.Index, id))
          using (var messageReader = FileSystem.OpenFileReader(FileType.Message, id))
          {
            index.BaseStream.Seek(localBegin, SeekOrigin.Begin);
            long beginOffset = index.ReadInt64();
            messageReader.BaseStream.Seek(beginOffset, SeekOrigin.Begin);
            message = new GruppoMessage();
            message.Offset = beginOffset;
            message.Timestamp = new DateTime(messageReader.ReadInt64());
            message.Meta = messageReader.ReadString();
            int length = messageReader.ReadInt32();
            message.Body = messageReader.ReadBytes(length);
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
          int id = (int)(offset / Settings.MaxMessagesInMessageFile) + 1;
          long localBegin = (offset - (id - 1) * Settings.MaxMessagesInMessageFile) * sizeof(Int64);
          using (var index = FileSystem.OpenFileReader(FileType.Index, id))
          using (var messageReader = FileSystem.OpenFileReader(FileType.Message, id))
          {
            index.BaseStream.Seek(localBegin, SeekOrigin.Begin);
            long beginOffset = index.ReadInt64();
            message = new GruppoMessage();
            message.Offset = beginOffset;
            message.Timestamp = new DateTime(messageReader.ReadInt64());
            message.Meta = messageReader.ReadString();
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
      int id = (int)(ProduceOffset / Settings.MaxMessagesInMessageFile);

      ProduceIndexWriter.Dispose();
      ProduceIndexWriter = FileSystem.OpenFileWriter(FileType.Index, id);
      ProduceMessageWriter = FileSystem.OpenFileWriter(FileType.Message, id);
    }
  }
}
