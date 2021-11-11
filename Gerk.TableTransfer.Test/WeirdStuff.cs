using System.Collections.Generic;

namespace Gerk.TableTransfer.Tests
{
	static class WeirdStuff
	{
		public static IEnumerable<T> InfiniteOf<T>(this T original)
		{
			while (true)
				yield return original;
		}
	}
}
