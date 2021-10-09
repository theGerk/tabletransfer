using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace TableTransfer
{
	public static class TableTransfer
	{
		public enum Type : UInt32
		{
			Bool,
			Uint8,
			Uint16,
			Uint32,
			Uint64,
			Int8,
			Int16,
			Int32,
			Int64,
			Decimal,
			Float32,
			Float64,
			Guid,
			String,
			BinaryData,
			DateTime,
			TimeSpan,
		}

		public static void Write(Stream stream, IEnumerable<IEnumerable<object>> values, IEnumerable<Type> types, IEnumerable<string> names = null)
		{
			using var writer = new BinaryWriter(stream, System.Text.Encoding.Default, true);

			// write version
			writer.Write((UInt32)Assembly.GetExecutingAssembly().GetName().Version.Major);

			uint columns = 0;

			// get types data buffered into memory stream so we can count the number of them before writing out to the actual stream.
			using (MemoryStream typeStream = new MemoryStream())
			{
				using (BinaryWriter typeWriter = new BinaryWriter(typeStream, System.Text.Encoding.Default, true))
					foreach (Type type in types)
					{
						typeWriter.Write((UInt32)type);
						columns++;
					}

				writer.Write(columns);

				// copy buffered type data into stream
				typeStream.Position = 0;
				typeStream.CopyTo(stream);
			}

			// write the column names now
		}
	}
}
