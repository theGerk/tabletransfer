using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Xunit;
using static Gerk.TableTransfer.TableTransfer;
using Type = Gerk.TableTransfer.TableTransfer.Type;
using Gerk.LinqExtensions;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gerk.TableTransfer.test
{
	public class UnitTest1
	{
		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		private static extern int memcmp(byte[] b1, byte[] b2, long count);

		private static bool ByteArrayCompare(byte[] b1, byte[] b2)
		{
			// Validate buffers are the same length.
			// This also ensures that the count does not exceed the length of either buffer.  
			return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
		}

		private static bool IsEqual(object a, object b)
		{
			if (a == null && b == null)
				return true;
			else if (a.GetType() != b.GetType())
				return false;
			else if (a is byte[] A && b is byte[] B)
				return ByteArrayCompare(A, B);
			else
				return Equals(a, b);
		}

		[Fact]
		public void FullTest()
		{
			var row = new (Type type, object value)[]
			{
				(Type.Bool, true),
				(Type.Bool, false),
				(Type.NullableBool, (bool?)false),
				(Type.NullableBool, (bool?)true),
				(Type.UInt8, (byte)39),
				(Type.UInt16, (ushort)394),
				(Type.UInt32, 3842U),
				(Type.UInt64,8321943UL),
				(Type.Int8, (sbyte)-34),
				(Type.Int16, (short)38),
				(Type.Int32, 0),
				(Type.Int64, 823433L),
				(Type.Decimal,42.42m),
				(Type.Float32, 42.42f),
				(Type.Float64, 42.42d),
				(Type.Guid, Guid.NewGuid()),
				(Type.String, "Hello world"),
				(Type.BinaryData, new byte[] { 0, 1, 1, 0, 1 }),
				(Type.DateTime, DateTime.Now),
				(Type.NullableDateTime, (DateTime?)DateTime.UtcNow),
				(Type.TimeSpan, TimeSpan.FromMinutes(1)),
				(Type.TimeSpan, TimeSpan.FromMinutes(-1)),
				(Type.NullableInt32, 42),
				(Type.NullableBinaryData, new byte[]{7, 7 }),
				(Type.NullableString, string.Empty),
				(Type.NullableBool, (bool?)null),
				(Type.NullableUInt8, (byte?)null),
				(Type.NullableUInt16, (ushort?)null),
				(Type.NullableUInt32, (uint?)null),
				(Type.NullableUInt64, (ulong?)null),
				(Type.NullableInt8, (sbyte?)null),
				(Type.NullableInt16, (short?)null),
				(Type.NullableInt32, (int?)null),
				(Type.NullableInt64, (long?)null),
				(Type.NullableDecimal, (decimal?)null),
				(Type.NullableFloat32, (float?)null),
				(Type.NullableFloat64, (double?)null),
				(Type.NullableGuid, (Guid?)null),
				(Type.NullableString, (string)null),
				(Type.NullableBinaryData, (byte[])null),
				(Type.NullableDateTime, (DateTime?)null),
				(Type.NullableTimeSpan, (TimeSpan?)null),
			};

			using MemoryStream stream = new MemoryStream();

			int count = 15859;
			Write(stream, row.Select(x => x.value).InfiniteOf().Take(count), row.Select(x => x.type).ToArray());

			stream.Position = 0;

			var read = Read(stream);

			if (read.types.Length != row.Length)
				throw new Exception("Lengths wrong");

			for (int i = 0; i < row.Length; i++)
			{
				if (read.types[i] != row[i].type)
					throw new Exception("Type issue");
			}

			if (read.names != null)
				throw new Exception("Names populated?");

			int c = 0;
			foreach (var _row in read.values.AsEnumerable())
			{
				if (_row.Length != row.Length)
					throw new Exception("Row length mismatch");
				c++;
				for (int i = 0; i < row.Length; i++)
				{
					if (!IsEqual(row[i].value, _row[i]))
						throw new Exception("Values wrong");
				}
			}
			if (c != count)
				throw new Exception("wrong row count");

			Assert.True(true);
		}

		[Fact]
		public async Task FullTestAsync()
		{
			var row = new (Type type, object value)[]
			{
				(Type.Bool, true),
				(Type.Bool, false),
				(Type.NullableBool, (bool?)false),
				(Type.NullableBool, (bool?)true),
				(Type.UInt8, (byte)39),
				(Type.UInt16, (ushort)394),
				(Type.UInt32, 3842U),
				(Type.UInt64,8321943UL),
				(Type.Int8, (sbyte)-34),
				(Type.Int16, (short)38),
				(Type.Int32, 0),
				(Type.Int64, 823433L),
				(Type.Decimal,42.42m),
				(Type.Float32, 42.42f),
				(Type.Float64, 42.42d),
				(Type.Guid, Guid.NewGuid()),
				(Type.String, "Hello world"),
				(Type.BinaryData, new byte[] { 0, 1, 1, 0, 1 }),
				(Type.DateTime, DateTime.Now),
				(Type.NullableDateTime, (DateTime?)DateTime.UtcNow),
				(Type.TimeSpan, TimeSpan.FromMinutes(1)),
				(Type.TimeSpan, TimeSpan.FromMinutes(-1)),
				(Type.NullableInt32, 42),
				(Type.NullableBinaryData, new byte[]{7, 7 }),
				(Type.NullableString, string.Empty),
				(Type.NullableBool, (bool?)null),
				(Type.NullableUInt8, (byte?)null),
				(Type.NullableUInt16, (ushort?)null),
				(Type.NullableUInt32, (uint?)null),
				(Type.NullableUInt64, (ulong?)null),
				(Type.NullableInt8, (sbyte?)null),
				(Type.NullableInt16, (short?)null),
				(Type.NullableInt32, (int?)null),
				(Type.NullableInt64, (long?)null),
				(Type.NullableDecimal, (decimal?)null),
				(Type.NullableFloat32, (float?)null),
				(Type.NullableFloat64, (double?)null),
				(Type.NullableGuid, (Guid?)null),
				(Type.NullableString, (string)null),
				(Type.NullableBinaryData, (byte[])null),
				(Type.NullableDateTime, (DateTime?)null),
				(Type.NullableTimeSpan, (TimeSpan?)null),
			};

			using MemoryStream stream = new MemoryStream();

			int count = 15859;
			await WriteAsync(stream, row.Select(x => x.value).InfiniteOf().Take(count), row.Select(x => x.type).ToArray());

			stream.Position = 0;

			var read = Read(stream);

			if (read.types.Length != row.Length)
				throw new Exception("Lengths wrong");

			for (int i = 0; i < row.Length; i++)
			{
				if (read.types[i] != row[i].type)
					throw new Exception("Type issue");
			}

			if (read.names != null)
				throw new Exception("Names populated?");

			int c = 0;
			foreach (var _row in read.values.AsEnumerable())
			{
				if (_row.Length != row.Length)
					throw new Exception("Row length mismatch");
				c++;
				for (int i = 0; i < row.Length; i++)
				{
					if (!IsEqual(row[i].value, _row[i]))
						throw new Exception("Values wrong");
				}
			}
			if (c != count)
				throw new Exception("wrong row count");

			Assert.True(true);
		}
	}
}
