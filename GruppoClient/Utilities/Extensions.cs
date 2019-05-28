using System;
using System.Collections.Generic;
using System.Text;

namespace GruppoClient.Utilities
{
  public static class Extensions
  {
    /// <summary>
    /// Check for null, if it's null throw an ArgumentNullException
    /// </summary>
    /// <typeparam name="T">Type of the object</typeparam>
    /// <param name="o">The object to be tested</param>
    /// <param name="name">The name of the parameter</param>
    /// <returns>The object that was tested</returns>
    public static T NotNull<T>(this T o, string name)
    {
      if ( o == null )
      {
        throw new ArgumentNullException(name);
      }

      return o;
    }

    public static string NotNullOrEmpty(this string s, string name)
    {
      if ( string.IsNullOrWhiteSpace(s))
      {
        throw new ArgumentNullException(name);
      }

      return s;
    }
  }
}
