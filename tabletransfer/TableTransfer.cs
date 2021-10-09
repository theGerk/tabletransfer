using Gerk.BinaryExtension;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace tabletransfer
{
	public static class TableTransfer
	{
		public enum Type : Int32
		{
			Bool,
			UInt8,
			UInt16,
			UInt32,
			UInt64,
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

		public struct FullType
		{
			public Type type;
			public bool nullable;

#if NETSTANDARD
			public static implicit operator FullType((Type type, bool nullable) arg) => new FullType() { type = arg.type, nullable = arg.nullable };
#endif
		}

		public struct ReadReturn
		{
			public FullType[] types;
			public string[] names;
			public IEnumerable<object[]> values;
		}


		/// <summary>
		///		Reads table from stream.
		///		<para>
		///			<b>Important:</b> Do not continue to read from <paramref name="stream"/> until the <seealso cref="ReadReturn.values"/> property has been fully enumerated.
		///		</para>
		/// </summary>
		/// <param name="stream">A readble stream at the start of a table encoded using the table transfer protocol</param>
		/// <returns></returns>
		public static ReadReturn Read(Stream stream)
		{
			var reader = new BinaryReader(stream, System.Text.Encoding.Default, true);
			try
			{
				ReadReturn output = new ReadReturn();
				// read version
				{
					var myVersion = Assembly.GetExecutingAssembly().GetName();
					var version = reader.ReadInt32();
					if (version != myVersion.Version.Major)
						throw new WrongVersionException(version, myVersion);
				}

				// read column count
				uint columns = reader.ReadUInt32();

				// read if headers are being included
				if (reader.ReadBoolean())
					output.names = new string[columns];
				else
					output.names = null;

				// read types in
				output.types = new FullType[columns];
				for (uint i = 0; i < columns; i++)
				{
					output.types[i].type = (Type)reader.ReadUInt32();
					output.types[i].nullable = reader.ReadBoolean();
				}

				// read column names if they exist
				if (output.names != null)
					for (uint i = 0; i < columns; i++)
						output.names[i] = reader.ReadString();


				output.values = ReadRows(reader, columns, output.types);
				return output;
			}
			catch
			{
				reader.Dispose();
				throw;
			}
		}

		/// <summary>
		/// Helper function for <see cref="Read(Stream)"/>. Reads through the rows yeild returning.
		/// </summary>
		/// <param name="reader">The reader object. Will need to be disposed at the end of this function.</param>
		/// <param name="columns"></param>
		/// <param name="types"></param>
		/// <returns></returns>
		private static IEnumerable<object[]> ReadRows(BinaryReader reader, uint columns, FullType[] types)
		{
			using (reader)
			{
				// variable is not currently used, but could be used for error/exception help and debugging
				uint rowNum = 0;
				while (reader.ReadBoolean())
				{
					object[] column = new object[columns];
					for (uint i = 0; i < columns; i++)
					{
						if (types[i].nullable)
							switch (types[i].type)
							{
								case Type.Bool:
									column[i] = reader.ReadNullableBoolean();
									break;
								case Type.UInt8:
									column[i] = reader.ReadNullableByte();
									break;
								case Type.UInt16:
									column[i] = reader.ReadNullableUInt16();
									break;
								case Type.UInt32:
									column[i] = reader.ReadNullableUInt32();
									break;
								case Type.UInt64:
									column[i] = reader.ReadNullableUInt64();
									break;
								case Type.Int8:
									column[i] = reader.ReadNullableSByte();
									break;
								case Type.Int16:
									column[i] = reader.ReadNullableInt16();
									break;
								case Type.Int32:
									column[i] = reader.ReadNullableInt32();
									break;
								case Type.Int64:
									column[i] = reader.ReadNullableInt64();
									break;
								case Type.Decimal:
									column[i] = reader.ReadNullableDecimal();
									break;
								case Type.Float32:
									column[i] = reader.ReadNullableSingle();
									break;
								case Type.Float64:
									column[i] = reader.ReadNullableDouble();
									break;
								case Type.Guid:
									column[i] = reader.ReadNullableGuid();
									break;
								case Type.String:
									column[i] = reader.ReadNullableString();
									break;
								case Type.BinaryData:
									column[i] = reader.ReadNullableBinaryData();
									break;
								case Type.DateTime:
									column[i] = reader.ReadNullableDateTime();
									break;
								case Type.TimeSpan:
									column[i] = reader.ReadNullableTimeSpan();
									break;
							}
						else
							switch (types[i].type)
							{
								case Type.Bool:
									column[i] = reader.ReadBoolean();
									break;
								case Type.UInt8:
									column[i] = reader.ReadByte();
									break;
								case Type.UInt16:
									column[i] = reader.ReadUInt16();
									break;
								case Type.UInt32:
									column[i] = reader.ReadUInt32();
									break;
								case Type.UInt64:
									column[i] = reader.ReadUInt64();
									break;
								case Type.Int8:
									column[i] = reader.ReadSByte();
									break;
								case Type.Int16:
									column[i] = reader.ReadInt16();
									break;
								case Type.Int32:
									column[i] = reader.ReadInt32();
									break;
								case Type.Int64:
									column[i] = reader.ReadInt64();
									break;
								case Type.Decimal:
									column[i] = reader.ReadDecimal();
									break;
								case Type.Float32:
									column[i] = reader.ReadSingle();
									break;
								case Type.Float64:
									column[i] = reader.ReadDouble();
									break;
								case Type.Guid:
									column[i] = reader.ReadGuid();
									break;
								case Type.String:
									column[i] = reader.ReadString();
									break;
								case Type.BinaryData:
									column[i] = reader.ReadBinaryData();
									break;
								case Type.DateTime:
									column[i] = reader.ReadDateTime();
									break;
								case Type.TimeSpan:
									column[i] = reader.ReadTimeSpan();
									break;
							}
					}
					yield return column;
					rowNum++;
				}
			}
		}

		/// <summary>
		/// Writes table to stream.
		/// </summary>
		/// <param name="stream"></param>
		/// <param name="values"></param>
		/// <param name="types"></param>
		/// <param name="names"></param>
		public static void Write(Stream stream, IEnumerable<IEnumerable<object>> values, IList<FullType> types, IEnumerable<string> names = null)
		{
			using (var writer = new BinaryWriter(stream, System.Text.Encoding.Default, true))
			{
				// write version
				writer.Write((Int32)Assembly.GetExecutingAssembly().GetName().Version.Major);

				uint columns = (uint)types.Count;

				// Write the number of columns
				writer.Write(columns);

				// Write if we have column names included
				writer.Write(names != null);

				// get types data buffered into memory stream so we can count the number of them before writing out to the actual stream.
				foreach (var fulltype in types)
				{
					var type = fulltype.type;
					var nullable = fulltype.nullable;
					writer.Write((UInt32)type);
					writer.Write(nullable);
				}

				// Write the column names if they are included
				if (names != null)
				{
					var nameEnumerator = names.GetEnumerator();
					for (uint i = 0; i < columns; i++)
					{
						if (!nameEnumerator.MoveNext())
							throw new Exception($"More elements in {nameof(types)} than there are in {nameof(names)}.");
						writer.Write(nameEnumerator.Current);
					}
					if (nameEnumerator.MoveNext())
						throw new Exception($"More elements in {nameof(names)} than there are in {nameof(types)}");
				}

				// Now go through values and write them all
				ulong rowNum = 0;
				foreach (var row in values)
				{
					// Write a 1 to indicate that there is another row.
					writer.Write(true);
					var columnEnumerator = row.GetEnumerator();
					for (int i = 0; i < columns; i++)
					{
						if (!columnEnumerator.MoveNext())
							throw new Exception($"More elements in {nameof(types)} than there are in row {rowNum}.");

						try
						{
							if (types[i].nullable)
								switch (types[i].type)
								{
									case Type.Bool:
										writer.Write((bool?)columnEnumerator.Current);
										break;
									case Type.UInt8:
										writer.Write((byte?)columnEnumerator.Current);
										break;
									case Type.UInt16:
										writer.Write((UInt16?)columnEnumerator.Current);
										break;
									case Type.UInt32:
										writer.Write((UInt32?)columnEnumerator.Current);
										break;
									case Type.UInt64:
										writer.Write((UInt64?)columnEnumerator.Current);
										break;
									case Type.Int8:
										writer.Write((sbyte?)columnEnumerator.Current);
										break;
									case Type.Int16:
										writer.Write((Int16?)columnEnumerator.Current);
										break;
									case Type.Int32:
										writer.Write((Int32?)columnEnumerator.Current);
										break;
									case Type.Int64:
										writer.Write((Int64?)columnEnumerator.Current);
										break;
									case Type.Decimal:
										writer.Write((decimal?)columnEnumerator.Current);
										break;
									case Type.Float32:
										writer.Write((float?)columnEnumerator.Current);
										break;
									case Type.Float64:
										writer.Write((double?)columnEnumerator.Current);
										break;
									case Type.Guid:
										writer.Write((Guid?)columnEnumerator.Current);
										break;
									case Type.String:
										writer.WriteNullable((string)columnEnumerator.Current);
										break;
									case Type.BinaryData:
										writer.WriteNullableBinaryData((byte[])columnEnumerator.Current);
										break;
									case Type.DateTime:
										writer.Write((DateTime?)columnEnumerator.Current);
										break;
									case Type.TimeSpan:
										writer.Write((TimeSpan?)columnEnumerator.Current);
										break;
								}
							else
								switch (types[i].type)
								{
									case Type.Bool:
										writer.Write((bool)columnEnumerator.Current);
										break;
									case Type.UInt8:
										writer.Write((byte)columnEnumerator.Current);
										break;
									case Type.UInt16:
										writer.Write((UInt16)columnEnumerator.Current);
										break;
									case Type.UInt32:
										writer.Write((UInt32)columnEnumerator.Current);
										break;
									case Type.UInt64:
										writer.Write((UInt64)columnEnumerator.Current);
										break;
									case Type.Int8:
										writer.Write((sbyte)columnEnumerator.Current);
										break;
									case Type.Int16:
										writer.Write((Int16)columnEnumerator.Current);
										break;
									case Type.Int32:
										writer.Write((Int32)columnEnumerator.Current);
										break;
									case Type.Int64:
										writer.Write((Int64)columnEnumerator.Current);
										break;
									case Type.Decimal:
										writer.Write((decimal)columnEnumerator.Current);
										break;
									case Type.Float32:
										writer.Write((float)columnEnumerator.Current);
										break;
									case Type.Float64:
										writer.Write((double)columnEnumerator.Current);
										break;
									case Type.Guid:
										writer.Write((Guid)columnEnumerator.Current);
										break;
									case Type.String:
										writer.Write((string)columnEnumerator.Current);
										break;
									case Type.BinaryData:
										writer.WriteBinaryData((byte[])columnEnumerator.Current);
										break;
									case Type.DateTime:
										writer.Write((DateTime)columnEnumerator.Current);
										break;
									case Type.TimeSpan:
										writer.Write((TimeSpan)columnEnumerator.Current);
										break;
								}
						}
						catch (Exception e)
						{
							throw new Exception($"Inner exception found at row: {rowNum}, col: {i}", e);
						}
					}
					if (columnEnumerator.MoveNext())
						throw new Exception($"More elements in row {rowNum} than there are in {nameof(types)}");
				}

				// Write a 0 to indicate no more rows
				writer.Write(false);
			}
		}
	}
}
