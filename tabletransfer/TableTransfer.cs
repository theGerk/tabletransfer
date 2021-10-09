using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace TableTransfer
{



	public static class TableTransfer
	{
		enum Type : UInt32
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

		public static async Task WriteAsync(Stream stream, IEnumerable<IEnumerable<object>> values, IEnumerable<Type> types, IEnumerable<string> names = null)
		{
			using BinaryWriter writer = new BinaryWriter(stream, System.Text.Encoding.Default, true);

			foreach (Type type in types)
			{
			}
		}
	}
}
