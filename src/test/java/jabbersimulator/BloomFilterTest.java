package jabbersimulator;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;

import org.xerial.snappy.Snappy;

import com.google.common.hash.Funnels;
import com.sangupta.murmur.Murmur3;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import trd.test.utilities.Utilities;

public class BloomFilterTest {

	public static long seed = 2 << 32 - 1;
	public static int maxSize = 10_000_000;
	public static double accuracy = 0.01;
	public static Integer[] readPoints = new Integer[] { 
			100_000, 200_000, 400_000, 800_000, 
			1_000_000, 2_000_000, 4_000_000, 8_000_000, 
			10_000_000 };

	public static long getHashOfRandomUUID() {
		UUID uuid = UUID.randomUUID();
		byte[] bUuid = Utilities.getUUIDAsByte(uuid);
		long hashedVal = Murmur3.hash_x86_32(bUuid, bUuid.length, seed);
		return hashedVal;
	}

	public static interface BFI {
		public String getName();
		public void create(String name, int maxSize, double accuracy);
		public void putRandomUUID();
		public String getCurrentSize();
	}
	
	public static class BF_Oretes implements BFI {
		BloomFilter<Long> bf; 
		String name;
		
		@Override
		public void create(String name, int maxSize, double accuracy) {
			this.bf = new FilterBuilder(maxSize, accuracy).buildBloomFilter();
			this.name = name;
		}

		@Override
		public void putRandomUUID() {
			bf.add(getHashOfRandomUUID());
		}

		@Override
		public String getCurrentSize() {
			
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream out = new ObjectOutputStream(bos);) {
				out.writeObject(bf);
				out.flush();
				byte[] outBuffer = Snappy.compress(bos.toByteArray());
				return String.format("%,8.2f", (double)outBuffer.length/1_000_000);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

		@Override
		public String getName() {
			return name;
		}
	}
	
	public static class BF_Guava implements BFI {
		com.google.common.hash.BloomFilter<Long> bf; 
		String name;
		
		@Override
		public void create(String name, int maxSize, double accuracy) {
			this.bf = com.google.common.hash.BloomFilter.create(Funnels.longFunnel(),maxSize, accuracy);
			this.name = name;
		}

		@Override
		public void putRandomUUID() {
			bf.put(getHashOfRandomUUID());
		}

		@Override
		public String getCurrentSize() {
			
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream out = new ObjectOutputStream(bos);) {
				bf.writeTo(out);
				out.flush();
				byte[] outBuffer = Snappy.compress(bos.toByteArray());
				return String.format("%,8.2f", (double)outBuffer.length/1_000_000);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

		@Override
		public String getName() {
			return name;
		}
	}
	
	
	public static void testSizing(String name, BFI aBloomFilter) {
		aBloomFilter.create(name, maxSize, accuracy);
		String[] readResults = new String[readPoints.length];
		int currReadPoint = 0;

		for (int i = 0; i <= maxSize; i++) {
			aBloomFilter.putRandomUUID();
			if (i == readPoints[currReadPoint]) {
				try {
					readResults[currReadPoint] = aBloomFilter.getCurrentSize();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				currReadPoint++;
			}
		}
		System.out.printf("Size of %s Bloom-Filter: %s\n", aBloomFilter.getName(), Arrays.toString(readResults));
	}

	
	public static void main(String[] args) {

//		testSizing("Oretes", new BF_Oretes());
//		testSizing("Guava ", new BF_Guava());
		
		BloomFilter<Long> bf1 = new FilterBuilder(100_000, accuracy).buildBloomFilter();
		BloomFilter<Long> bf2 = new FilterBuilder(1_000_000, accuracy).buildBloomFilter();

		for (long i = 0L; i < 2000; i++)
			bf1.add(i);
		System.out.printf("%f\n", bf1.getEstimatedPopulation());
			
		for (long i = 3000L; i < 4000; i++)
			bf2.add(i);
		System.out.printf("%f\n", bf2.getEstimatedPopulation());
		
		bf2.union(bf1);
		System.out.printf("%f\n", bf2.getEstimatedPopulation());
	}
}
