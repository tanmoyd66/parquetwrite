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

import trd.test.utilities.Utilities.Config;
import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
//				System.out.printf("Reading file %s under latch:%d\n", path, cdl.hashCode());
				readParquetFile(new Work(path, useColumnIndexFilter, customerId, wg));
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
//				System.out.printf("Marking down latch:%d\n", cdl.hashCode());
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
	
	public static void readParquetFile(Work work) throws IOException, InterruptedException {
		Path inPath = new Path(work.path);
		File file = new File(inPath.toString());
		if (!file.exists())
			return;
		
//		FilterPredicate actualFilter = eq(longColumn("customerid"), work.customerId);
		int count = 0;
		try (ParquetReader<Group> reader = ParquetReader
											.builder(new GroupReadSupport(), inPath)
//											.withFilter(FilterCompat.get(actualFilter))
//											.useColumnIndexFilter(work.useColumnIndexFilter)
											.build()) {
			Group group;
			while ((group = reader.read()) != null) {
				Binary binVal = group.getBinary("installationId", 0);
				if (binVal != null) {
					byte[] bVal = binVal.getBytes();
					long hashedVal = Murmur3.hash_x86_32(bVal, bVal.length, Generator.seed);
					synchronized (work.wg) {
						work.wg.bfGlobal.put(hashedVal);
						work.wg.count.incrementAndGet();
						count++;
					}
				}
			}
		}
//		System.out.printf("%s : Count: %d Actual: %d, Bloom: %d\n", inPath.getName(), count, work.wg.count.get(), work.wg.bfGlobal.approximateElementCount());
	}
	
	public static long getCountsForDaysFromFiles(Config config, long customerId, int year, List<Tuples.Pair<Integer, Integer>> probeList) throws InterruptedException {
		
//		System.out.printf("%s\n", Arrays.toString(probeList.toArray()));
		WorkGlobal wg = new WorkGlobal(config);
		ConcurrentLinkedQueue<Work> workQueue = new ConcurrentLinkedQueue<>();
		int numThreads = 30;
		
		// Get list of files from probe list and create work
		int countOfWorkItems = 0;
		for (Tuples.Pair<Integer, Integer> probeFile : probeList) {
			String dir = String.format("%s/%s/%s/%s", config.directory, year, probeFile.a, probeFile.b);			
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
//		Thread.sleep(1000);
//		System.out.printf("%d, %d\n", wg.count.get(), wg.bfGlobal.approximateElementCount());
		return wg.count.get();
	}	

	public static long getCountsForDaysFromFilesUsingThreadPool(
							Config config, 
							ThreadPoolExecutor tpe, 
							long customerId, 
							int year, 
							List<Tuples.Pair<Integer, Integer>> probeList) throws InterruptedException {
		
		WorkGlobal wg = new WorkGlobal(config);
		int numTasks = probeList.size();
		
		List<String> fileToScan = new ArrayList<>();
		for (int i = 0; i < numTasks; i++) {
			Tuples.Pair<Integer, Integer> probeFile = probeList.get(i);
			String dirPath = String.format("%s/%s/%s/%s/%s", config.directory, customerId, year, probeFile.a, probeFile.b);
			File dir = new File(dirPath);
			File[] filesInDir = Utilities.listFiles(dir);
			if (filesInDir != null) {
				for (File file : filesInDir) {
					String filePath = file.getAbsolutePath();
					if (filePath.endsWith(".parquet")) {
						fileToScan.add(file.getAbsolutePath());
					}
				}
			}
		}

		if (fileToScan.size() > 0) {
			CountDownLatch cdl = new CountDownLatch(fileToScan.size());
			for (String file : fileToScan)
				tpe.execute(new PerThreadCollectorTask(cdl, file, true, customerId, wg));
			cdl.await();
			return wg.bfGlobal.approximateElementCount();
		}
		
		return 0;
		
//		Object[] filesScanned = fileToScan.toArray();
//		System.out.printf("Scanning %d files:\n",fileToScan.size());
//		for (Object o : filesScanned)
//			System.out.println(o);
//		
//		// Perform parallel scan and count
//		cdl.await();
//		return wg.bfGlobal.approximateElementCount();
	}	
	
	public static long getDistinctCountFromFile(
							trd.test.utilities.Utilities.Config config, 
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
					String p = config.directory + "/" + customerId + "/" + ldi.getStorageDirectory();
					if (new File(p).exists())
						probeList.add(new Tuples.Pair<Integer, Integer>(ldi.month, ldi.date));
				}
			} else if (start.date == -1) {
				for (int i = 1; i <= 31; i++) {
					LocalDateInfo ldi = new LocalDateInfo(start.year, start.month, i);
					String p = config.directory + "/" + customerId + "/" + ldi.getStorageDirectory();
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
					CountDistinctByScan.getCountsForDaysFromFiles(config, customerId, start.year, probeList): 
					CountDistinctByScan.getCountsForDaysFromFilesUsingThreadPool(config, tpe, customerId, start.year, probeList);
	}
}
