using System.Collections.Generic;
using System.IO;

namespace Gruppo.Storage
{
  public class HardDriveFileSystem : IFileSystem
  {
    private readonly string MessageDirectory;
    private readonly string GroupIndexDirectory;

    public HardDriveFileSystem(string baseDir, string topic, int messageSplitSize)
    {
      MessageSplitSize = messageSplitSize;
      TopicDirectory = Path.Combine(baseDir, "topics", topic);
      MessageDirectory = Path.Combine(TopicDirectory, "messages");
      GroupIndexDirectory = Path.Combine(TopicDirectory, "group_indexes");

      Directory.CreateDirectory(MessageDirectory);
      Directory.CreateDirectory(GroupIndexDirectory);
    }

    public string TopicDirectory { get; private set; }
    public int MessageSplitSize { get; }

    public IEnumerable<string> EnumerateGroupNames()
    {
      return Directory.EnumerateFiles(GroupIndexDirectory);
    }

    public IEnumerable<string> EnumerateMessageFiles()
    {
      return Directory.EnumerateFiles(MessageDirectory);

    }

    public long FileSize(string fileName)
    {
      return (new FileInfo(fileName)).Length;
    }

    public string FileName(int id)
    {
      return Path.Combine(MessageDirectory, $"messages_{id:00000}.bin");
    }

    public BinaryReader OpenGroupIndexFileReader(string groupName)
    {
      string path = Path.Combine(GroupIndexDirectory, $"{groupName}.bin");
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite));
    }

    public BinaryWriter OpenGroupIndexFileWriter(string groupName)
    {
      string path = Path.Combine(GroupIndexDirectory, $"{groupName}.bin");
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read));
    }

    public BinaryWriter OpenMessageFileWriter(long offset)
    {
      int id = (int)(offset / MessageSplitSize);
      string path = Path.Combine(MessageDirectory, FileName(id));
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryReader OpenMessageFileReader(long offset)
    {
      int id = (int)(offset / MessageSplitSize);
      string path = Path.Combine(MessageDirectory, FileName(id));
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryWriter OpenIndexWriter()
    {
      string path = Path.Combine(MessageDirectory, "index.bin");
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }

    public BinaryReader OpenIndexReader()
    {
      string path = Path.Combine(MessageDirectory, "index.bin");
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }
  }
}
