package trd.test.utilities;

public class Tuples {
	public static class Pair<A,B> {
		public A a;
		public B b;
		
		public Pair(A a, B b) {
			this.a = a;
			this.b = b;
		}
		
		public String toString() {
			return String.format("(%s,%s)", a, b);
		}
	}

	public static class Triple<A,B,C> {
		public A a;
		public B b;
		public C c;
		
		public Triple(A a, B b, C c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}

		public String toString() {
			return String.format("(%s,%s, %s)", a, b, c);
		}
	}
}
