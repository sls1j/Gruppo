using Gruppo.SDK.Communications;
using NUnit.Framework;

namespace GruppoTests
{
  public class MessageEnvelopeTests
  {
    [Test]
    public void HeaderSerializingTest()
    {
      MessageHeader expectedHeader = new MessageHeader()
      {
        Version = "9.99",
        MetaSize = 100,
        BodySize = 1024 * 1024,
      };

      byte[] header = expectedHeader.SerializeHeader();
      MessageHeader actualHeader = MessageHeader.DeserializeHeader(header);

      Assert.AreEqual(header.Length, 18);
      Assert.AreEqual(expectedHeader.Version, actualHeader.Version);
      Assert.AreEqual(expectedHeader.MetaSize, actualHeader.MetaSize);
      Assert.AreEqual(expectedHeader.BodySize, actualHeader.BodySize);
    }
  }
}
