using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Gruppo.Storage
{
  public enum FileType { Index, Message};
  public interface IFileSystem
  {
    string TopicDirectory { get; }

    long FileSize(string fileName);
    string FileName(FileType fileType, int id);

    IEnumerable<string> EnumerateFiles(FileType type);
    Stream OpenFileStream(FileType type, int id);
    BinaryWriter OpenFileWriter(FileType type, int id);
    BinaryReader OpenFileReader(FileType type, int id);

    IEnumerable<string> EnumerateGroupIndexFiles();
    BinaryWriter OpenGroupIndexFileWriter(string groupName);
    BinaryReader OpenGroupIndexFileReader(string groupName);
  }
}
