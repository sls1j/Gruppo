using System.Collections.Generic;
using System.IO;

namespace Gruppo.Storage
{
  public class HardDriveFileSystem : IFileSystem
  {
    private readonly string IndexDirectory;
    private readonly string MessageDirectory;
    private readonly string GroupIndexDirectory;

    public HardDriveFileSystem(string baseDir, string topic)
    {
      TopicDirectory = Path.Combine(baseDir, "topics", topic);
      IndexDirectory = Path.Combine(TopicDirectory, "index");
      MessageDirectory = Path.Combine(TopicDirectory, "messages");
      GroupIndexDirectory = Path.Combine(TopicDirectory, "group_indexes");

      Directory.CreateDirectory(IndexDirectory);
      Directory.CreateDirectory(MessageDirectory);
      Directory.CreateDirectory(GroupIndexDirectory);
    }

    public string TopicDirectory { get; private set; }

    public IEnumerable<string> EnumerateGroupIndexFiles()
    {
      return Directory.EnumerateFiles(GroupIndexDirectory);
    }

    public IEnumerable<string> EnumerateIndexFiles()
    {
      return Directory.EnumerateFiles(IndexDirectory);
    }

    public IEnumerable<string> EnumerateFiles(FileType fileType)
    {
      switch (fileType)
      {
        default:
        case FileType.Message:
          return Directory.EnumerateFiles(MessageDirectory);
        case FileType.Index:
          return Directory.EnumerateFiles(IndexDirectory);
      }
    }

    public long FileSize(string fileName)
    {
      return (new FileInfo(fileName)).Length;
    }

    public string FileName(FileType type, int id)
    {
      switch (type)
      {
        case FileType.Index:
          return Path.Combine(IndexDirectory, $"index_{id:00000}.bin");
        default:
        case FileType.Message:
          return Path.Combine(MessageDirectory, $"messages_{id:00000}.bin");
      }
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

    public Stream OpenFileStream(FileType type, int id)
    {
      string path = FileName(type, id);
      return new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
    }

    public BinaryReader OpenFileReader(FileType type, int id)
    {
      string path = FileName(type, id);
      return new BinaryReader(new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite));
    }

    public BinaryWriter OpenFileWriter(FileType type, int id)
    {
      string path = FileName(type, id);
      return new BinaryWriter(new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
    }
  }
}
