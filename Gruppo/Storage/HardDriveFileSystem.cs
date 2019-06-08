using System;
using System.Collections.Generic;
using System.IO;

namespace Gruppo.Storage
{
  public class HardDriveFileSystem : IFileSystem
  {
    private readonly string _messageDirectory;
    private readonly string _groupIndexDirectory;
    private bool _isActive;

    public HardDriveFileSystem(string baseDir, string topic, int messageSplitSize)
    {
      MessageSplitSize = messageSplitSize;
      TopicDirectory = Path.Combine(baseDir, "topics", topic);
      _messageDirectory = Path.Combine(TopicDirectory, "messages");
      _groupIndexDirectory = Path.Combine(TopicDirectory, "group_indexes");
    }

    public void Activate()
    {
      Directory.CreateDirectory(_messageDirectory);
      Directory.CreateDirectory(_groupIndexDirectory);
      _isActive = true;
    }

    public string TopicDirectory { get; private set; }
    public int MessageSplitSize { get; }

    private void TestForActive()
    {
      if (!_isActive)
        throw new InvalidOperationException("Activate must be called first.");
    }

    public IEnumerable<string> EnumerateGroupNames()
    {
      TestForActive();
      return Directory.EnumerateFiles(_groupIndexDirectory);
    }

    public IEnumerable<string> EnumerateMessageFiles()
    {
      TestForActive();
      return Directory.EnumerateFiles(_messageDirectory);

    }

    public long FileSize(string fileName)
    {
      return (new FileInfo(fileName)).Length;
    }

    public string FileName(int id)
    {
      TestForActive();
      return Path.Combine(_messageDirectory, $"messages_{id:00000}.bin");
    }

    public BinaryReader OpenGroupIndexFileReader(string groupName)
    {
      TestForActive();
      string path = Path.Combine(_groupIndexDirectory, $"{groupName}.bin");
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite));
    }

    public BinaryWriter OpenGroupIndexFileWriter(string groupName)
    {
      TestForActive();
      string path = Path.Combine(_groupIndexDirectory, $"{groupName}.bin");
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read));
    }

    public BinaryWriter OpenMessageFileWriter(long offset)
    {
      TestForActive();
      int id = (int)(offset / MessageSplitSize);
      string path = Path.Combine(_messageDirectory, FileName(id));
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryReader OpenMessageFileReader(long offset)
    {
      TestForActive();
      int id = (int)(offset / MessageSplitSize);
      string path = Path.Combine(_messageDirectory, FileName(id));
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryWriter OpenIndexWriter()
    {
      TestForActive();
      string path = Path.Combine(_messageDirectory, "index.bin");
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryReader OpenIndexReader()
    {
      TestForActive();
      string path = Path.Combine(_messageDirectory, "index.bin");
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }
  }
}
