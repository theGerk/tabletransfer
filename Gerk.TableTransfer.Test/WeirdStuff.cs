using System.Collections.Generic;

namespace Gerk.tabletransfer.test
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
