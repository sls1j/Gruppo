using System;
using System.IO;

namespace Gruppo.SDK.Communications
{
  /// <summary>
  /// The header of a socket message.  This contains all of the information for reading the whole message from the socket
  /// </summary>
  public class MessageHeader
  {
    public MessageHeader()
    {
      _Version = "1.00";
    }

    private string _Version;

    /// <summary>
    /// The version of the message header
    /// </summary>
    public string Version
    {
      get { return _Version; }
      set
      {
        if (string.IsNullOrEmpty(value) || value.Length != 4)
          throw new ArgumentOutOfRangeException(nameof(Version), $"Version must be 4 ASCII characters.");

        _Version = value;
      }
    }

    /// <summary>
    /// The size in bytes of the meta data of the message.
    /// </summary>
    public ushort MetaSize;

    /// <summary>
    /// The size in bytes of the body section of a message
    /// </summary>
    public ulong BodySize;

    /// <summary>
    /// The size of the header when it is serialized.
    /// </summary>
    public const int BinarySize = 18;

    /// <summary>
    /// Build the header of the message envelope
    /// </summary>
    /// <returns>The header as a byte array</returns>
    public byte[] SerializeHeader()
    {
      byte[] header = new byte[18];
      using (var ms = new MemoryStream(header))
      using (var writer = new BinaryWriter(ms))
      {
        for (int i = 0; i < 4; i++)
          writer.Write((byte)_Version[i]);
        writer.Write(MetaSize);
        writer.Write(BodySize);
      }
      return header;
    }

    /// <summary>
    /// Builds a header of the message envelope
    /// </summary>
    /// <param name="headerBytes"></param>
    /// <returns></returns>
    public static MessageHeader DeserializeHeader(byte[] headerBytes)
    {
      using (MemoryStream ms = new MemoryStream(headerBytes))
      using (BinaryReader reader = new BinaryReader(ms))
      {
        var msg = new MessageHeader();

        msg._Version = new string(reader.ReadChars(4));
        msg.MetaSize = reader.ReadUInt16();
        msg.BodySize = reader.ReadUInt64();
        return msg;
      }
    }
  }
}
