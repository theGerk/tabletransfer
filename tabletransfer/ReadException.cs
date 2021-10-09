using System;
using System.Reflection;
using System.Runtime.Serialization;

namespace TableTransfer
{
	public abstract class ReadException : Exception
	{
		public ulong BytePosition { get; }


		protected ReadException(ulong position) : base()
		{
			BytePosition = position;
		}
		protected ReadException(string message, ulong position) : base(message)
		{
			BytePosition = position;
		}
		protected ReadException(string message, Exception innerException, ulong position) : base(message, innerException)
		{
			BytePosition = position;
		}
		protected ReadException(SerializationInfo serialization, StreamingContext streamingContext, ulong position) : base(serialization, streamingContext)
		{
			BytePosition = position;
		}
	}


	public class WrongVersionException : ReadException
	{
		public int VersionRead { get; }
		public int CurrentVersion { get; }

		public WrongVersionException(int readVersion, AssemblyName currentVersion)
			: base((currentVersion.Version.Major < readVersion) ? $"Need to update {currentVersion.Name} to major version {readVersion} in order to parse the given data. Currently you are on version {currentVersion.Version.Major}." : $"Reading data with out of date encoding. Data is on major version {readVersion} and you are on version {currentVersion.Version.Major}. Downgrade {currentVersion.Name} to parse the data.", 0)
		{
			VersionRead = readVersion;
			CurrentVersion = currentVersion.Version.Major;
		}
	}
}
