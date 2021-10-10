using System.Collections;
using System.Collections.Generic;

namespace tabletransfer
{
	internal class SkipFirstMoveNextEnumerator<T> : IEnumerator<T>
	{
		readonly IEnumerator<T> enumerator;
		bool skippedYet = false;

		public object Current => enumerator.Current;

		T IEnumerator<T>.Current => enumerator.Current;

		public SkipFirstMoveNextEnumerator(IEnumerator<T> enumerator) { this.enumerator = enumerator; }

		public bool MoveNext()
		{
			if (!skippedYet)
				return skippedYet = true;
			else
				return enumerator.MoveNext();
		}

		public void Reset() => enumerator.Reset();

		public void Dispose() => enumerator.Dispose();
	}
}
