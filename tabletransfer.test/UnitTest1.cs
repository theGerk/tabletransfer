using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Xunit;
using static tabletransfer.TableTransfer;
using Type = tabletransfer.TableTransfer.Type;
using System.Collections.Generic;

namespace tabletransfer.test
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
			var row = new (FullType type, object value)[]
			{
				((Type.Bool, false), true),
				((Type.Bool, false), false),
				((Type.Bool, true), (bool?)false),
				((Type.Bool, true), (bool?)true),
				((Type.UInt8, false), (byte)39),
				((Type.UInt16, false), (ushort)394),
				((Type.UInt32, false), 3842U),
				((Type.UInt64, false),8321943UL),
				((Type.Int8, false), (sbyte)-34),
				((Type.Int16, false), (short)38),
				((Type.Int32, false), 0),
				((Type.Int64, false), 823433L),
				((Type.Decimal, false),42.42m),
				((Type.Float32, false), 42.42f),
				((Type.Float64, false), 42.42d),
				((Type.Guid, false), Guid.NewGuid()),
				((Type.String, false), "Hello world"),
				((Type.BinaryData, false), new byte[] { 0, 1, 1, 0, 1 }),
				((Type.DateTime, false), DateTime.Now),
				((Type.DateTime, true), (DateTime?)DateTime.UtcNow),
				((Type.TimeSpan, false), TimeSpan.FromMinutes(1)),
				((Type.TimeSpan, false), TimeSpan.FromMinutes(-1)),
				((Type.Int32, true), 42),
				((Type.BinaryData, true), new byte[]{7, 7 }),
				((Type.String, true), string.Empty),
				((Type.Bool, true), (bool?)null),
				((Type.UInt8, true), (byte?)null),
				((Type.UInt16, true), (ushort?)null),
				((Type.UInt32, true), (uint?)null),
				((Type.UInt64, true), (ulong?)null),
				((Type.Int8, true), (sbyte?)null),
				((Type.Int16, true), (short?)null),
				((Type.Int32, true), (int?)null),
				((Type.Int64, true), (long?)null),
				((Type.Decimal, true), (decimal?)null),
				((Type.Float32, true), (float?)null),
				((Type.Float64, true), (double?)null),
				((Type.Guid, true), (Guid?)null),
				((Type.String, true), (string)null),
				((Type.BinaryData, true), (byte[])null),
				((Type.DateTime, true), (DateTime?)null),
				((Type.TimeSpan, true), (TimeSpan?)null),
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
				if (read.types[i].nullable != row[i].type.nullable || read.types[i].type != row[i].type.type)
					throw new Exception("Type issue");
			}

			if (read.names != null)
				throw new Exception("Names populated?");

			int c = 0;
			foreach (var _row in read.values)
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
