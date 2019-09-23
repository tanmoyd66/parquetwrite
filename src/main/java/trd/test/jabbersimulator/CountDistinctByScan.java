package trd.test.jabbersimulator;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.sangupta.murmur.Murmur3;

import trd.test.jabbersimulator.JabberSimulator.Config;
import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;

public class CountDistinctByScan {

	public static class WorkGlobal {
		final BloomFilter<Long> bfGlobal;
		final AtomicLong count;
		public WorkGlobal(Config config) {
			this.bfGlobal = BloomFilter.create(Funnels.longFunnel(), config.bfSize, 0.001);
			this.count = new AtomicLong(0L);
		}
	}
	
	public static class Work {
		String path;
		final boolean useColumnIndexFilter;
		final long customerId;
		final WorkGlobal wg;
		
		public Work(String path, boolean useColumnIndexFilter, long customerId, WorkGlobal wg) {
			this.path = path;
			this.useColumnIndexFilter = useColumnIndexFilter;
			this.customerId = customerId;
			this.wg = wg;
		}
	}

	private static void Sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static class PerThreadCollectorTask implements Runnable {
		final String path;
		final boolean useColumnIndexFilter;
		final long customerId;
		final WorkGlobal wg;
		final CountDownLatch cdl;
		
		public PerThreadCollectorTask(CountDownLatch cdl, String path, boolean useColumnIndexFilter, long customerId, WorkGlobal wg) {
			this.path = path;
			this.useColumnIndexFilter = useColumnIndexFilter;
			this.customerId = customerId;
			this.wg = wg;
			this.cdl = cdl;
		}
		
		@Override
		public void run() {
			try {
				readParquetFile(new Work(path, useColumnIndexFilter, customerId, wg));
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				cdl.countDown();
			}
		}
	}	
	public static class PerThreadCollector implements Runnable {
		final ConcurrentLinkedQueue<Work> workQueue;
		final CountDownLatch cdl;
		
		public PerThreadCollector(ConcurrentLinkedQueue<Work> workQueue, CountDownLatch cdl) {
			this.workQueue = workQueue;
			this.cdl = cdl;
		}
		
		@Override
		public void run() {
			while (true) {
				try {
					Work work = workQueue.poll();
					if (work == null) {
						Sleep(100);
						continue;
					} else if (work.path == null) {
						break;
					}
					readParquetFile(work);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			cdl.countDown();
		}
		
	}	
	
	public static void readParquetFile(Work work) throws IOException {
		Path inPath = new Path(work.path);
		File file = new File(inPath.toString());
		if (!file.exists())
			return;
		
		FilterPredicate actualFilter = eq(longColumn("customerid"), work.customerId);
		try (ParquetReader<Group> reader = ParquetReader
											.builder(new GroupReadSupport(), inPath)
											.withFilter(FilterCompat.get(actualFilter))
											.useColumnIndexFilter(work.useColumnIndexFilter)
											.build()) {
			Group group;
			while ((group = reader.read()) != null) {
				Binary binVal = group.getBinary("installationId", 0);
				if (binVal != null) {
					byte[] bVal = binVal.getBytes();
					long hashedVal = Murmur3.hash_x86_32(bVal, bVal.length, Generator.seed);
					work.wg.bfGlobal.put(hashedVal);
					work.wg.count.incrementAndGet();
				}
			}
		} 
	}
	
	public static long getCountsForDaysFromFiles(String directory, Config config, long customerId, int year, List<Tuples.Pair<Integer, Integer>> probeList) throws InterruptedException {
		
//		System.out.printf("%s\n", Arrays.toString(probeList.toArray()));
		WorkGlobal wg = new WorkGlobal(config);
		ConcurrentLinkedQueue<Work> workQueue = new ConcurrentLinkedQueue<>();
		long startTime = System.nanoTime();
		int numThreads = 30;
		
		// Get list of files from probe list and create work
		int countOfWorkItems = 0;
		for (Tuples.Pair<Integer, Integer> probeFile : probeList) {
			String dir = String.format("%s/%s/%s/%s", directory, year, probeFile.a, probeFile.b);			
			Work work = new Work(
							dir,
							true,
							customerId,
							wg);
			workQueue.offer(work);
			countOfWorkItems++;
		}
		
		// Find out the number of threads to create
		numThreads = Math.min(numThreads, countOfWorkItems);
		
		// Create threads and sentinnels to read from the queues and do work
		CountDownLatch cdl = new CountDownLatch(numThreads);
		for (int i = 0; i < numThreads; i++) {
			workQueue.add(new Work(null, true, customerId, null));
			new Thread(new PerThreadCollector(workQueue, cdl)).start();
		}
		
		// Perform parallel scan and count
		cdl.await();
		
//		System.out.printf("%d, %d\n", wg.count.get(), wg.bfGlobal.approximateElementCount());
		return wg.count.get();
	}	

	public static long getCountsForDaysFromFilesUsingThreadPool(String directory, Config config, 
																ThreadPoolExecutor tpe, 
																long customerId, 
																int year, 
																List<Tuples.Pair<Integer, Integer>> probeList) throws InterruptedException {
		
		WorkGlobal wg = new WorkGlobal(config);
		int numTasks = probeList.size();
		
		CountDownLatch cdl = new CountDownLatch(numTasks);
		for (int i = 0; i < numTasks; i++) {
			Tuples.Pair<Integer, Integer> probeFile = probeList.get(i);
			String dir = String.format("%s/%s/%s/%s", directory, year, probeFile.a, probeFile.b);			
			tpe.execute(new PerThreadCollectorTask(cdl, dir, true, customerId, wg));
		}
		
		// Perform parallel scan and count
		cdl.await();
		return wg.count.get();
	}	
	
	public static long getDistinctCountFromFile(
							String directory, 
							Config config, 
							long customerId, 
							ThreadPoolExecutor tpe,
							LocalDateInfo start, 
							LocalDateInfo end) 
			throws SQLException, IOException, InterruptedException {
		List<Tuples.Pair<Integer, Integer>> probeList = new ArrayList<>();		
		if (start.month == end.month) {
			if (start.month == -1) {
				assert start.date == -1;
				LocalDateInfo startLD = new LocalDateInfo(start.year, 1, 1);
				LocalDateInfo endLD   = new LocalDateInfo(start.year, 12, 31);
				List<LocalDateInfo> ldInfoList = LocalDateInfo.getAllLocalDateInfosBetween2Dates(startLD, endLD);
				for (LocalDateInfo ldi : ldInfoList) {
					String p = directory + "/" + ldi.getStorageDirectory();
					if (new File(p).exists())
						probeList.add(new Tuples.Pair<Integer, Integer>(ldi.month, ldi.date));
				}
			} else if (start.date == -1) {
				for (int i = 1; i <= 31; i++) {
					LocalDateInfo ldi = new LocalDateInfo(start.year, start.month, i);
					String p = directory + "/" + ldi.getStorageDirectory();
					if (new File(p).exists())
						probeList.add(new Tuples.Pair<Integer, Integer>(start.month, i));
				}
			} else {
				for (int i = start.date; i <= end.date; i++) {
					probeList.add(new Tuples.Pair<Integer, Integer>(start.month, i));
				}
			}
		} else {
			// Add the remaining days of the start month
			for (int i = start.date; i <= 31; i++) {
				probeList.add(new Tuples.Pair<Integer, Integer>(start.month, i));
			}
			
			// Add full months if needed
			for (int i = start.month + 1; i < end.month; i++) {
				for (int j = 0; j < 31; j++)
					probeList.add(new Tuples.Pair<Integer, Integer>(i, j));
			}
			
			// Add the remaining days of the end month
			for (int i = 1; i <= end.date; i++) {
				probeList.add(new Tuples.Pair<Integer, Integer>(end.month, i));
			}
		}
		return tpe == null ? 
					CountDistinctByScan.getCountsForDaysFromFiles(directory, config, customerId, start.year, probeList): 
					CountDistinctByScan.getCountsForDaysFromFilesUsingThreadPool(directory, config, tpe, customerId, start.year, probeList);
	}
}
