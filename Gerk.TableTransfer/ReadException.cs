using System;
using System.Reflection;
using System.Runtime.Serialization;

namespace Gerk.tabletransfer
{
	/// <summary>
	/// Exception raised when parsing serialized table.
	/// </summary>
	public abstract class ReadException : Exception
	{
		/// <summary>
		/// Byte position of error.
		/// </summary>
		public ulong BytePosition { get; }

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="position">The byte position of the error.</param>
		protected ReadException(ulong position) : base()
		{
			BytePosition = position;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="message">The message that describes the error.</param>
		/// <param name="position">The byte position of the error.</param>
		protected ReadException(string message, ulong position) : base(message)
		{
			BytePosition = position;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="message">The message that describes the error.</param>
		/// <param name="innerException">The exception that is the cause of the current exception, or a null reference (<c>Nothing</c> in Visual Basic) if no inner exception is specified.</param>
		/// <param name="position">The byte position of the error.</param>
		protected ReadException(string message, Exception innerException, ulong position) : base(message, innerException)
		{
			BytePosition = position;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="streamingContext">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <param name="position">The byte position of the error.</param>
		protected ReadException(SerializationInfo info, StreamingContext streamingContext, ulong position) : base(info, streamingContext)
		{
			BytePosition = position;
		}
	}

	/// <summary>
	/// Exception raised by reading a table serialized in a different version. Versions are defined by the assembly version's major number and each increment of that will be a breaking change.
	/// </summary>
	public class WrongVersionException : ReadException
	{
		/// <summary>
		/// The version being read.
		/// </summary>
		public int VersionRead { get; }

		/// <summary>
		/// The version of us. 
		/// </summary>
		public int CurrentVersion { get; }

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="readVersion">The version being read.</param>
		/// <param name="currentVersion">The current version we are on.</param>
		public WrongVersionException(int readVersion, AssemblyName currentVersion)
			: base((currentVersion.Version.Major < readVersion) ? $"Need to update {currentVersion.Name} to major version {readVersion} in order to parse the given data. Currently you are on version {currentVersion.Version.Major}." : $"Reading data with out of date encoding. Data is on major version {readVersion} and you are on version {currentVersion.Version.Major}. Downgrade {currentVersion.Name} to parse the data.", 0)
		{
			VersionRead = readVersion;
			CurrentVersion = currentVersion.Version.Major;
		}
	}
}
