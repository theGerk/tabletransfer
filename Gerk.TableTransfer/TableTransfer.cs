using Gerk.BinaryExtension;
using Gerk.LinqExtensions;
using Gerk.SpecialDataReaders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Gerk.tabletransfer
{
	public static class TableTransfer
	{
		/// <summary>
		/// Enum for types that can be included in a table.
		/// <para>
		///		The least significant bit in the enum identifies weather or not the value is a nullable.
		///		<list type="bullet">
		///			<item><c>..xxx0</c> means that the value describes a nonnullable type.</item>
		///			<item><c>..xxx1</c> means that the value descirbes a nullable type.</item>
		///		</list>
		/// </para>
		/// </summary>
		public enum Type : byte
		{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
			// This is setup so that the least significant bit is always able to signify if it is a nullable type or not.
			// ..xxx0 => not nullable
			// ..xxx1 => nullable
			// May consider changing this to using seperate boolean field in the future if there is more metadata.

			Bool,
			NullableBool,
			UInt8,
			NullableUInt8,
			UInt16,
			NullableUInt16,
			UInt32,
			NullableUInt32,
			UInt64,
			NullableUInt64,
			Int8,
			NullableInt8,
			Int16,
			NullableInt16,
			Int32,
			NullableInt32,
			Int64,
			NullableInt64,
			Decimal,
			NullableDecimal,
			Float32,
			NullableFloat32,
			Float64,
			NullableFloat64,
			Guid,
			NullableGuid,
			String,
			NullableString,
			BinaryData,
			NullableBinaryData,
			DateTime,
			NullableDateTime,
			TimeSpan,
			NullableTimeSpan,
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
		}

		private static void WriteType(this BinaryWriter writer, Type type) => writer.Write((byte)type);
		private static Type ReadType(this BinaryReader reader) => (Type)reader.ReadByte();

		private static readonly Dictionary<System.Type, Type> mapping = new Dictionary<System.Type, Type>()
		{
			[typeof(bool)] = Type.Bool,
			[typeof(bool?)] = Type.NullableBool,
			[typeof(byte)] = Type.UInt8,
			[typeof(byte?)] = Type.NullableUInt8,
			[typeof(UInt16)] = Type.UInt16,
			[typeof(UInt16?)] = Type.NullableUInt16,
			[typeof(UInt32)] = Type.UInt32,
			[typeof(UInt32?)] = Type.NullableUInt32,
			[typeof(UInt64)] = Type.UInt64,
			[typeof(UInt64?)] = Type.NullableUInt64,
			[typeof(sbyte)] = Type.Int8,
			[typeof(sbyte?)] = Type.NullableInt8,
			[typeof(Int16)] = Type.Int16,
			[typeof(Int16?)] = Type.NullableInt16,
			[typeof(Int32)] = Type.Int32,
			[typeof(Int32?)] = Type.NullableInt32,
			[typeof(Int64)] = Type.Int64,
			[typeof(Int64?)] = Type.NullableInt64,
			[typeof(Decimal)] = Type.Decimal,
			[typeof(Decimal?)] = Type.NullableDecimal,
			[typeof(float)] = Type.Float32,
			[typeof(float?)] = Type.NullableFloat32,
			[typeof(double)] = Type.Float64,
			[typeof(double?)] = Type.NullableFloat64,
			[typeof(Guid)] = Type.Guid,
			[typeof(Guid?)] = Type.NullableGuid,
			[typeof(String)] = Type.NullableString,
			[typeof(byte[])] = Type.NullableBinaryData,
			[typeof(DateTime)] = Type.DateTime,
			[typeof(DateTime?)] = Type.NullableDateTime,
			[typeof(TimeSpan)] = Type.TimeSpan,
			[typeof(TimeSpan?)] = Type.NullableTimeSpan,
		};

		/// <summary>
		/// Helper function for <see cref="Read(Stream)"/>. Reads through the rows yeild returning.
		/// </summary>
		/// <param name="reader">The reader object. Will need to be disposed at the end of this function.</param>
		/// <param name="columns"></param>
		/// <param name="types"></param>
		/// <returns></returns>
		private static IEnumerable<object[]> ReadRows(BinaryReader reader, uint columns, Type[] types)
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
						switch (types[i])
						{
							case Type.NullableBool:
								column[i] = reader.ReadNullableBoolean();
								break;
							case Type.NullableUInt8:
								column[i] = reader.ReadNullableByte();
								break;
							case Type.NullableUInt16:
								column[i] = reader.ReadNullableUInt16();
								break;
							case Type.NullableUInt32:
								column[i] = reader.ReadNullableUInt32();
								break;
							case Type.NullableUInt64:
								column[i] = reader.ReadNullableUInt64();
								break;
							case Type.NullableInt8:
								column[i] = reader.ReadNullableSByte();
								break;
							case Type.NullableInt16:
								column[i] = reader.ReadNullableInt16();
								break;
							case Type.NullableInt32:
								column[i] = reader.ReadNullableInt32();
								break;
							case Type.NullableInt64:
								column[i] = reader.ReadNullableInt64();
								break;
							case Type.NullableDecimal:
								column[i] = reader.ReadNullableDecimal();
								break;
							case Type.NullableFloat32:
								column[i] = reader.ReadNullableSingle();
								break;
							case Type.NullableFloat64:
								column[i] = reader.ReadNullableDouble();
								break;
							case Type.NullableGuid:
								column[i] = reader.ReadNullableGuid();
								break;
							case Type.NullableString:
								column[i] = reader.ReadNullableString();
								break;
							case Type.NullableBinaryData:
								column[i] = reader.ReadNullableBinaryData();
								break;
							case Type.NullableDateTime:
								column[i] = reader.ReadNullableDateTime();
								break;
							case Type.NullableTimeSpan:
								column[i] = reader.ReadNullableTimeSpan();
								break;
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
		/// Basic output from reading table.
		/// </summary>
		public struct ReadReturn
		{
			/// <summary>
			/// The types for each column in order
			/// </summary>
			public Type[] types;
			/// <summary>
			/// The names of each column in order. Is <see langword="null"/> when names were not included.
			/// </summary>
			public string[] names;
			/// <summary>
			/// Emuerates the rows. Each element is an array of objects representing values in order.
			/// </summary>
			public IEnumerable<object[]> values;

			public int Columns => types.Length;

			public IDataReader ToDataReader()
			{
				var dataReader = new EnumeratorDataReader();
				if (names == null)
				{
					for (int i = 0; i < Columns; i++)
					{
						int c = i;
						dataReader.Set(null, values, x => x[c], null);
					}
				}
				else
				{
					for (int i = 0; i < Columns; i++)
					{
						int c = i;
						dataReader.Set(names[c], values, x => x[c], null);
					}
				}

				return dataReader;
			}
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
				output.types = new Type[columns];
				for (uint i = 0; i < columns; i++)
				{
					output.types[i] = reader.ReadType();
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
		/// Writes table to stream.
		/// </summary>
		/// <param name="stream"></param>
		/// <param name="values"></param>
		/// <param name="types"></param>
		/// <param name="names"></param>
		/// <typeparam name="RowType"></typeparam>
		public static void Write<RowType>(Stream stream, IEnumerator<RowType> values, IList<Type> types, IEnumerable<string> names = null) where RowType : IEnumerable
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
				foreach (var type in types)
				{
					writer.WriteType(type);
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
				while (values.MoveNext())
				{
					// Write a 1 to indicate that there is another row.
					writer.Write(true);
					var columnEnumerator = values.Current.GetEnumerator();
					for (int i = 0; i < columns; i++)
					{
						if (!columnEnumerator.MoveNext())
							throw new Exception($"More elements in {nameof(types)} than there are in row {rowNum}.");

						try
						{
							switch (types[i])
							{
								case Type.NullableBool:
									writer.Write((bool?)columnEnumerator.Current);
									break;
								case Type.NullableUInt8:
									writer.Write((byte?)columnEnumerator.Current);
									break;
								case Type.NullableUInt16:
									writer.Write((UInt16?)columnEnumerator.Current);
									break;
								case Type.NullableUInt32:
									writer.Write((UInt32?)columnEnumerator.Current);
									break;
								case Type.NullableUInt64:
									writer.Write((UInt64?)columnEnumerator.Current);
									break;
								case Type.NullableInt8:
									writer.Write((sbyte?)columnEnumerator.Current);
									break;
								case Type.NullableInt16:
									writer.Write((Int16?)columnEnumerator.Current);
									break;
								case Type.NullableInt32:
									writer.Write((Int32?)columnEnumerator.Current);
									break;
								case Type.NullableInt64:
									writer.Write((Int64?)columnEnumerator.Current);
									break;
								case Type.NullableDecimal:
									writer.Write((decimal?)columnEnumerator.Current);
									break;
								case Type.NullableFloat32:
									writer.Write((float?)columnEnumerator.Current);
									break;
								case Type.NullableFloat64:
									writer.Write((double?)columnEnumerator.Current);
									break;
								case Type.NullableGuid:
									writer.Write((Guid?)columnEnumerator.Current);
									break;
								case Type.NullableString:
									writer.WriteNullable((string)columnEnumerator.Current);
									break;
								case Type.NullableBinaryData:
									writer.WriteNullableBinaryData((byte[])columnEnumerator.Current);
									break;
								case Type.NullableDateTime:
									writer.Write((DateTime?)columnEnumerator.Current);
									break;
								case Type.NullableTimeSpan:
									writer.Write((TimeSpan?)columnEnumerator.Current);
									break;
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

		public static void Write<RowType>(Stream stream, IEnumerable<RowType> values, IList<Type> types, IEnumerable<string> names = null) where RowType : IEnumerable
			=> Write(stream, values.GetEnumerator(), types, names);

		public static void Write<RowType>(Stream stream, IEnumerator<RowType> values, IEnumerable<string> names = null) where RowType : IEnumerable
		{
			if (values.MoveNext())
			{
				IEnumerator<RowType> enumerate()
				{
					// Does do a move next before the first yield return as the first move next will hav elready happened by the time this code has been reached
					do
						yield return values.Current;
					while (values.MoveNext());
				}

				var types = values.Current.Cast<object>().Select(x => mapping[x.GetType()]).ToArray();
				Write(stream, enumerate(), types, names);
			}
			else
				WriteNonTable(stream);
		}

		public static void Write<RowType>(Stream stream, IEnumerable<RowType> values, IEnumerable<string> names = null) where RowType : IEnumerable
			=> Write(stream, values.GetEnumerator(), names);

		private static void WriteNonTable(Stream stream)
		{
#if NET461_OR_GREATER
			Write(stream, Array.Empty<IEnumerable>(), Array.Empty<Type>());
#else
			Write(stream, new IEnumerable[0], new Type[0]);
#endif
		}

		public static void Write(Stream stream, IDataReader dataReader, IDictionary<string, Type> typeMapping, bool includeNames = true)
		{
			// Helper function for iterating through the dataReader as Enumerator
			IEnumerator<object[]> enumerate()
			{
				object[] objs = new object[dataReader.FieldCount];

				// Doesn't do a read first because first read is happending in outer function.
				do
				{
					dataReader.GetValues(objs);
					yield return objs;
				} while (dataReader.Read());
			}

			if (dataReader.Read())
			{
				var names = new string[dataReader.FieldCount];
				var types = new Type[dataReader.FieldCount];
				for (int i = 0; i < dataReader.FieldCount; i++)
				{
					names[i] = dataReader.GetName(i);
					if (includeNames && names[i] == null)
						throw new Exception($"Column {i}: Can not have null names. Either don't include names or set names properly.");
					types[i] = typeMapping[dataReader.GetDataTypeName(i)];
				}

				Write(stream, enumerate(), types, includeNames ? names : null);
			}
			else
				WriteNonTable(stream);
		}
	}
}
