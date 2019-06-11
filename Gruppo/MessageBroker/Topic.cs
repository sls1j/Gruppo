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
    private ExecutionGuard _guard;
    private IFileSystem _fileSystem;
    private long _produceOffset;
    private BinaryWriter _produceIndexWriter;
    private BinaryWriter _produceMessageWriter;
    private Dictionary<string, Group> _groupConsumers;


    public Topic(string topicName, IFileSystem fileSystem)
    {
      if (!Regex.IsMatch(topicName, @"^[\d\-\.a-z_]*$"))
        throw new InvalidTopicNameException($"'{topicName} is not valid.");

      fileSystem.Activate();

      _guard = new ExecutionGuard();

      Name = topicName.NotNullOrEmpty(nameof(topicName));
      _fileSystem = fileSystem.NotNull(nameof(fileSystem));
      _groupConsumers = new Dictionary<string, Group>();
      LoadGroupIndexs();

      // find the product offset on startup
      _produceOffset = 0;
      using (var reader = _fileSystem.OpenIndexReader())
      {
        if (reader.BaseStream.Length >= sizeof(long))
        {
          reader.BaseStream.Seek(sizeof(long), SeekOrigin.End);
          _produceOffset = reader.ReadInt64();
        }
      }

      _produceIndexWriter = _fileSystem.OpenIndexWriter();
      _produceIndexWriter.BaseStream.Seek(0, SeekOrigin.End);

      _produceMessageWriter = _fileSystem.OpenMessageFileWriter(_produceOffset);
      _produceMessageWriter.BaseStream.Seek(0, SeekOrigin.End);
    }

    private void LoadGroupIndexs()
    {
      foreach (var groupName in _fileSystem.EnumerateGroupNames())
      {
        Group g = CreateGroup(groupName);

        _groupConsumers.Add(groupName, g);
      }
    }

    private Group CreateGroup(string groupName)
    {
      Group g = new Group();
      g.Name = groupName;
      using (var reader = _fileSystem.OpenGroupIndexFileReader(groupName))
      {
        g.GroupIndexWriter = _fileSystem.OpenGroupIndexFileWriter(g.Name);
        g.Offset = (reader.BaseStream.Length < sizeof(Int64)) ? 0 : reader.ReadInt64();
        g.IndexReader = _fileSystem.OpenIndexReader();
        g.MessageReader = _fileSystem.OpenMessageFileReader(g.Offset);

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

    public void SetOffset(string groupName, int offset)
    {
      Group group;
      lock (_groupConsumers)
      {
        if (!_groupConsumers.TryGetValue(groupName, out group))
        {
          group = CreateGroup(groupName);
          _groupConsumers.Add(groupName, group);
        }
      }

      throw new NotImplementedException();
    }

    public string Name { get; private set; }

    public void Consume(string groupName, out GruppoMessage message)
    {
      if (_guard.EnterExecute())
      {
        try
        {
          Group group;
          lock (_groupConsumers)
          {
            if (!_groupConsumers.TryGetValue(groupName, out group))
            {
              group = CreateGroup(groupName);
              _groupConsumers.Add(groupName, group);
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
                group.MessageReader = _fileSystem.OpenMessageFileReader(group.Offset);
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
          _guard.ExitExecute();
        }
      }
      else
        message = null;
    }

    public void Consume(long offset, out GruppoMessage message)
    {
      if (_guard.EnterExecute())
      {
        try
        {
          using (var index = _fileSystem.OpenIndexReader())
          using (var messageReader = _fileSystem.OpenMessageFileReader(offset))
          {
            index.BaseStream.Seek((offset) * sizeof(Int64), SeekOrigin.Begin);
            long beginOffset = index.ReadInt64();
            messageReader.BaseStream.Seek(beginOffset, SeekOrigin.Begin);
            message = ReadMessage(messageReader, offset, true);
          }
        }
        finally
        {
          _guard.ExitExecute();
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
      if (_produceIndexWriter != null)
      {
        _produceIndexWriter.Dispose();
        _produceIndexWriter = null;
      }

      if (_produceMessageWriter != null)
      {
        _produceMessageWriter.Dispose();
        _produceIndexWriter = null;
      }

      foreach (var group in _groupConsumers.Values)
      {
        group.GroupIndexWriter.Dispose();
        group.IndexReader.Dispose();
        group.MessageReader.Dispose();
      }

      _groupConsumers.Clear();

      GC.SuppressFinalize(this);
    }

    public TopicStatistics GetStats()
    {
      lock (this)
      {
        return new TopicStatistics()
        {
          Name = Name,
          StorageDirectory = _fileSystem.TopicDirectory,
          MessageCount = _produceOffset
        };
      }
    }

    public void Peek(long offset, out GruppoMessage message)
    {
      if (_guard.EnterExecute())
      {
        try
        {
          using (var index = _fileSystem.OpenIndexReader())
          using (var messageReader = _fileSystem.OpenMessageFileReader(offset))
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
          _guard.ExitExecute();
        }
      }
      else
        message = null;
    }

    public bool Produce(GruppoMessage message, out long offset, out DateTime timestamp)
    {
      if (_guard.EnterExecute())
      {
        try
        {
          lock (this)
          {
            // write out the message to the message stream
            timestamp = DateTime.UtcNow;
            long beginOffset = _produceMessageWriter.BaseStream.Position;
            _produceMessageWriter.Write(timestamp.Ticks);
            _produceMessageWriter.Write(message.Meta ?? string.Empty);
            if (message.Body == null)
              _produceMessageWriter.Write((int)0);
            else
            {
              _produceMessageWriter.Write(message.Body.Length);
              _produceMessageWriter.Write(message.Body);
            }
            _produceMessageWriter.Flush();

            offset = _produceOffset++;

            // write message start position to the index
            _produceIndexWriter.Write(beginOffset);
            _produceIndexWriter.Flush();
          }

          // check to see if the file needs to be rolled
          if (_produceOffset % _fileSystem.MessageSplitSize == 0)
          {
            CreateProduceWriters();
          }

          return true;
        }
        finally
        {
          _guard.ExitExecute();
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
      _produceMessageWriter.Dispose();
      _produceMessageWriter = _fileSystem.OpenMessageFileWriter(_produceOffset);
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
