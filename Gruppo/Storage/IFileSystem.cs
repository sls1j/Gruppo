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
    int MessageSplitSize { get; }

    long FileSize(string fileName);
    string FileName(int id);

    IEnumerable<string> EnumerateMessageFiles();
    BinaryWriter OpenMessageFileWriter(long offset);
    BinaryReader OpenMessageFileReader(long offset);
    BinaryWriter OpenIndexWriter();
    BinaryReader OpenIndexReader();

    IEnumerable<string> EnumerateGroupNames();
    BinaryWriter OpenGroupIndexFileWriter(string groupName);
    BinaryReader OpenGroupIndexFileReader(string groupName);
  }
}
