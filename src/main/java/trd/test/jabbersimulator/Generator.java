package trd.test.jabbersimulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.xerial.snappy.Snappy;

import com.google.common.base.Strings;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.sangupta.murmur.Murmur3;

import trd.test.utilities.Utilities.Config;
import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.IntStream;

import javax.sql.DataSource;

public class Generator {
	public static long seed = 137234567L;

	
	
	public static class GeneratorData {
		List<Short>     platform = new ArrayList<>();
		List<UUID>      orgId = new ArrayList<>();
		List<Short>     productName = new ArrayList<>();
		List<Integer>   productVersion = new ArrayList<>();
		List<Long>	    customerId = new ArrayList<>();
		List<Short>     type = new ArrayList<>();
		List<UUID>      installationId = new ArrayList<>();
		Map<Long, BloomFilter<Long>> bfMapGlobal = new ConcurrentHashMap<>();

		public synchronized void update(Config config, Map<Long, BloomFilter<Long>> bfMap) {
			for (Map.Entry<Long, BloomFilter<Long>> bfEntry : bfMap.entrySet()) {
				BloomFilter<Long> bf = null;
				if ((bf = bfMapGlobal.get(bfEntry.getKey())) == null) {
					bf = BloomFilter.create(Funnels.longFunnel(), config.bfSize, 0.001);
					bfMapGlobal.put(bfEntry.getKey(), bf);
				}
				bf.putAll(bfEntry.getValue());
			}
		}
		
		public synchronized void reset() {
			bfMapGlobal.clear();
		}
	}
	
	public static class Work {
		int id;
		int countPerFile;
		final String file;
		final GeneratorData gd;
		final Date start;
		final Date end;
		final LocalDateInfo ldInfo;
		final Config config;
		
		public Work(String file, int id, int countPerFile, GeneratorData gd, LocalDateInfo ldInfo, Date start, Date end, Config config) {
			this.file = file;
			this.id = id;
			this.countPerFile = countPerFile;
			this.gd = gd;
			this.start = start;
			this.end = end;
			this.ldInfo = ldInfo;
			this.config = config;
		}
	}

	private static void Sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	private static byte[] bfToBytes(BloomFilter<Long> bf) throws IOException {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			 bf.writeTo(os);
			 os.flush();
			 return Snappy.compress(os.toByteArray());
		}
	}

	public static ByteArrayOutputStream bfToByteStream(BloomFilter<Long> bf) throws IOException {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			 bf.writeTo(os);
			 os.flush();
			 return os;
		}
	}

	public static BloomFilter<Long> bytesToBf(byte[] _bfBytes) throws IOException {
		 byte[] bfBytes = Snappy.uncompress(_bfBytes);
		try (ByteArrayInputStream is = new ByteArrayInputStream(bfBytes)) {
			 return BloomFilter.readFrom(is, Funnels.longFunnel());
		}
	}
	
	public static class BFDBUpdator implements Runnable {
		final CountDownLatch cdl;
		final Map<Long, BloomFilter<Long>> bfMap;
		final LocalDateInfo ldInfo;
		final DataSource ds;
		final Config config;
		
		public BFDBUpdator(Config config, CountDownLatch cdl, Map<Long, BloomFilter<Long>> bfMap, LocalDateInfo ldInfo, DataSource ds) {
			this.cdl = cdl;
			this.bfMap = bfMap;
			this.ldInfo = ldInfo;
			this.ds = ds;
			this.config = config;
		}
		
		@Override
		public void run() {
			try (Connection conn = ds.getConnection()) {
				updateBloomFilterToDB(config, conn, ldInfo, bfMap);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			cdl.countDown();
		}
	}

	public static class BFDBThreadPoolUpdator implements Runnable {
		final CountDownLatch cdl;
		final Map<Long, BloomFilter<Long>> bfMap;
		final LocalDateInfo ldInfo;
		final DataSource ds;
		final Config config;
		
		public BFDBThreadPoolUpdator(Config config, CountDownLatch cdl, Map<Long, BloomFilter<Long>> bfMap, LocalDateInfo ldInfo, DataSource ds) {
			this.cdl = cdl;
			this.bfMap = bfMap;
			this.ldInfo = ldInfo;
			this.ds = ds;
			this.config = config;
		}
		
		@Override
		public void run() {
			try (Connection conn = ds.getConnection()) {
				updateBloomFilterToDB(config, conn, ldInfo, bfMap);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			cdl.countDown();
		}
	}
	
	private static void updateBloomFilterToDB(
							Config config, 
							DataSource ds, 
							LocalDateInfo ldInfo, 
							Map<Long, BloomFilter<Long>> bfMap) 
			throws InterruptedException {
		
		List<Map<Long, BloomFilter<Long>>> listBfMap = new ArrayList<>();
		
		for (int i = 0; i < config.threadPoolSize; i++) {
			listBfMap.add(new HashMap<>());
		}
		
		for (Map.Entry<Long, BloomFilter<Long>> me : bfMap.entrySet()) {
			int threadNumber = (int)(me.getKey() % config.threadPoolSize);
			listBfMap.get(threadNumber).put(me.getKey(), me.getValue());
		}
		bfMap.clear();
		
		CountDownLatch cdl = new CountDownLatch(config.threadPoolSize);
		for (int i = 0; i < config.threadPoolSize; i++) {
			new Thread(new BFDBUpdator(config, cdl, listBfMap.get(i), ldInfo, ds)).start();
		}
		cdl.await();
	}

	private static void updateBloomFilterToDBUsingThreadPoolExecutor(
							Config config,
							DataSource ds, 
							LocalDateInfo ldInfo, 
							Map<Long, BloomFilter<Long>> bfMap, 
							ThreadPoolExecutor tpe) throws InterruptedException {
		
		List<Map<Long, BloomFilter<Long>>> listBfMap = new ArrayList<>();
		
		for (int i = 0; i < config.threadPoolSize; i++) {
			listBfMap.add(new HashMap<>());
		}
		
		for (Map.Entry<Long, BloomFilter<Long>> me : bfMap.entrySet()) {
			int threadNumber = (int)(me.getKey() % config.threadPoolSize);
			listBfMap.get(threadNumber).put(me.getKey(), me.getValue());
		}
		bfMap.clear();
				
		CountDownLatch cdl = new CountDownLatch(config.threadPoolSize);
		for (int i = 0; i < config.threadPoolSize; i++) {
			tpe.execute(new BFDBThreadPoolUpdator(config, cdl, listBfMap.get(i), ldInfo, ds));
		}
		cdl.await();
	}
	
	public static Tuples.Pair<String, BloomFilter<Long>> getBloomFilterFromRowSet(ResultSet rs, int index) 
			throws SQLException, IOException {
		if (rs == null || rs.getString(index) == null)
			return null;
		String loc = rs.getString(index);
		byte[] bfBytes = Files.readAllBytes(Paths.get(loc));
		if (bfBytes != null)
			return new Tuples.Pair<>(loc, bytesToBf(bfBytes));
		return null;
	}

	public static void saveBloomFilterToPreparedStatement(Config config, PreparedStatement ps, int index, Tuples.Pair<String, BloomFilter<Long>> tupleVal) 
					throws SQLException, IOException {
		
		if (tupleVal == null || tupleVal.b == null)
			return;
		
		if (Strings.isNullOrEmpty(tupleVal.a)) {
			tupleVal.a = config.bloomFilterLoc + "/" + UUID.randomUUID().toString();
		}
		
		// Write bloom filter in the location
		byte[] bfData = bfToBytes(tupleVal.b);
		Files.write(Paths.get(tupleVal.a), bfData);
		ps.setString(index, tupleVal.a);
	}
	
	@SuppressWarnings("unchecked")
	private static void updateBloomFilterToDB(Config config, Connection conn, LocalDateInfo ldInfo, Map<Long, BloomFilter<Long>> bfMap) throws IOException, SQLException {
		
		// Fail Fast
		if (bfMap.entrySet().size() == 0)
			return;
		
		// Create SQL Statement to get the information from the database
		String sqlSelectFmt = "select customerid, countdata, year, month, day from countdistinctoninstanceid where customerid in (%s) and year = %d and month in (%d, -1) and day in (%d, -1)";
		StringBuilder customerList = new StringBuilder();
		boolean fFirst = true;
		for (Map.Entry<Long, BloomFilter<Long>> me : bfMap.entrySet()) {
			if (!fFirst) 
				customerList.append(", ");
			else
				fFirst = false;
			customerList.append(me.getKey());
		}

		// Build the maps in memory for processing
		Map<Long, Tuples.Pair<String, BloomFilter<Long>>> bfDBMapDate = new HashMap<>();
		Map<Long, Tuples.Pair<String, BloomFilter<Long>>> bfDBMapMTD  = new HashMap<>();
		Map<Long, Tuples.Pair<String, BloomFilter<Long>>> bfDBMapYTD  = new HashMap<>();
		
		// Get the data from the database, including aggregates
		String sqlSelect = String.format(sqlSelectFmt, customerList.toString(), ldInfo.year, ldInfo.month, ldInfo.date);
		ResultSet rs = conn.createStatement().executeQuery(sqlSelect);
		if (rs != null) {
			while (rs.next()) {
				Long customerId = rs.getLong(1);
				int year  = rs.getInt(3);
				int month = rs.getInt(4);
				int date  = rs.getInt(5);				
				Tuples.Pair<String, BloomFilter<Long>> bfTuple = getBloomFilterFromRowSet(rs, 2);
				if (bfTuple == null || bfTuple.b != null) {
					if (date == -1) {
						if (month == -1)
							bfDBMapYTD.put(customerId, bfTuple);
						else 
							bfDBMapMTD.put(customerId, bfTuple);
					} else {
						bfDBMapDate.put(customerId, bfTuple);						
					}
				}
			}
		}
		
		// Now update all the bloom-filters in memory
		for (Map.Entry<Long, BloomFilter<Long>> me : bfMap.entrySet()) {
			Tuples.Pair<String, BloomFilter<Long>> bfDate = bfDBMapDate.get(me.getKey());
			if (bfDate != null) {
				bfDate.b.putAll(me.getValue());
			} else 
				bfDBMapDate.put(me.getKey(), new Tuples.Pair<>(null, me.getValue()));
			
			Tuples.Pair<String, BloomFilter<Long>> bfMTD = bfDBMapMTD.get(me.getKey());
			if (bfMTD != null) {
				bfMTD.b.putAll(me.getValue());
			} else 
				bfDBMapMTD.put(me.getKey(), new Tuples.Pair<>(null, me.getValue()));
			
			Tuples.Pair<String, BloomFilter<Long>> bfYTD = bfDBMapYTD.get(me.getKey());
			if (bfYTD != null) {
				bfYTD.b.putAll(me.getValue());
			} else 
				bfDBMapYTD.put(me.getKey(), new Tuples.Pair<>(null, me.getValue()));
		}		
		
		// Now update the database with the values
		conn.setAutoCommit(false);
		String sqlUpdate = "replace into countdistinctoninstanceid(customerid, year, month, day, countdata, count) values (?, ?, ?, ?, ?, ?)";
		int updateCount = 0, totalUpdateCount = 0;
		
		try (PreparedStatement ps = conn.prepareStatement(sqlUpdate)) {
			for (Object _map : new Object[] {bfDBMapDate, bfDBMapMTD, bfDBMapYTD} ) {
				Map<Long, Tuples.Pair<String, BloomFilter<Long>>> map = (Map<Long, Tuples.Pair<String, BloomFilter<Long>>>)_map;
				
				for (Map.Entry<Long, Tuples.Pair<String, BloomFilter<Long>>> me : map.entrySet()) {
					
					ps.setLong(1, me.getKey());
					ps.setInt(2, ldInfo.year);
					
					if (map == bfDBMapYTD) {
						ps.setInt(3, -1);
						ps.setInt(4, -1);
					} else if (map == bfDBMapMTD) {
						ps.setInt(3, ldInfo.month);
						ps.setInt(4, -1);
					} else {
						ps.setInt(3, ldInfo.month);
						ps.setInt(4, ldInfo.date);
					}
					saveBloomFilterToPreparedStatement(config, ps, 5, me.getValue());
					
					int count = 0;
					try {
						count = (int)me.getValue().b.approximateElementCount();
					} catch (Exception ex) {
//						ex.printStackTrace();
					}
					ps.setInt(6, count);
					totalUpdateCount++;
					updateCount++;
					ps.addBatch();
					if (updateCount >= 1000) {
						ps.executeBatch();
						conn.commit();
						ps.clearBatch();
						updateCount = 0;
					}
				}
			}
			try {
				if (updateCount > 0) {
					ps.executeBatch();
					conn.commit();
					ps.clearBatch();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
//		System.out.printf("Total Updates:%d\n", totalUpdateCount);
	}

	public static class PerThreadGenerator implements Runnable {
		final ConcurrentLinkedQueue<Work> workQueue;
		final CountDownLatch cdl;
		
		public PerThreadGenerator(ConcurrentLinkedQueue<Work> workQueue, CountDownLatch cdl) {
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
					} else if (work.id == -1) {
						break;
					}
					generateParquetFile(work);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			cdl.countDown();
		}
		
		HashMap<Long, ParquetWriter<Group>> parquetWriterMap = new HashMap<>();
		
		@SuppressWarnings("deprecation")
		private synchronized ParquetWriter<Group> getParquetWriter(MessageType schema, Config conf, String file, long customerId, LocalDateInfo ldInfo) throws IOException {
			ParquetWriter<Group> pw = parquetWriterMap.get(customerId);
			if (pw == null) {
				Configuration configuration = new Configuration();
				GroupWriteSupport.setSchema(schema, configuration);

				// Create directories for year, month and date
				String directory = createDirectories(conf, customerId, ldInfo);
				Path fsPath = new Path(directory + "/" + file);
				
				pw = new ParquetWriter<Group>(
						fsPath, 
						new GroupWriteSupport(),
						CompressionCodecName.UNCOMPRESSED, 
						ParquetWriter.DEFAULT_BLOCK_SIZE, 
						ParquetWriter.DEFAULT_PAGE_SIZE,
						1024, 
						true, 
						false, 
						ParquetProperties.WriterVersion.PARQUET_2_0, 
						configuration);
				parquetWriterMap.put(customerId, pw);
			}
			return pw;
		}
		
		private void closeAll() throws IOException {
			for (Map.Entry<Long, ParquetWriter<Group>> me : parquetWriterMap.entrySet()) {
				if (me.getValue() != null) {
					me.getValue().close();
				}
			}
		}
		
		@SuppressWarnings({ "deprecation", "unused" })
		private void generateParquetFile(Work work) throws IOException, ParseException, SQLException {
			
			String parquetFile = work.file + ".parquet";
			MessageType schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("message ActiveUsers { "
									+ "required int32   platform; " 
									+ "required binary  orgId; " 
									+ "required int32   productName; "
									+ "required int32   productVersion; " 
									+ "required int64   customerid; "
									+ "required int32   type; "
									+ "required binary  installationId; "
									+ "required int64   time;" + "} ");
			
			long start = System.nanoTime();
			Map<Long, BloomFilter<Long>> bloomMap = new HashMap<>();
			Random rand = new Random();
			try {	
				int printInterval = work.countPerFile / 10;
				for (int i = 0; i < work.countPerFile; i++) {
					
					long time = Math.min(work.start.getTime() + i, work.end.getTime());					
					int next = Math.abs(rand.nextInt());
					
					Long customerId = work.gd.customerId.get(next % (work.gd.customerId.size()));
					ParquetWriter<Group> pw = getParquetWriter(schema, work.config, parquetFile, customerId, work.ldInfo);					
					
					UUID installationIdUuid = work.gd.installationId.get(Math.abs(rand.nextInt() % (work.gd.installationId.size() - 1)));
					byte[] installationId = Utilities.getUUIDAsByte(installationIdUuid);
					SimpleGroupFactory fact = new SimpleGroupFactory(schema);
					Group group = fact
									.newGroup()
									.append("platform", work.gd.platform.get(next % (work.gd.platform.size())))
									.append("orgId", Binary.fromByteArray(Utilities.getUUIDAsByte(work.gd.orgId.get(next % (work.gd.orgId.size())))))
									.append("productName", work.gd.productName.get(next % (work.gd.productName.size())))
									.append("productVersion", work.gd.productVersion.get(next % (work.gd.productVersion.size())))
									.append("customerid", customerId)
									.append("type", work.gd.type.get(next % (work.gd.type.size())))
									.append("installationId", Binary.fromByteArray(installationId))
									.append("time", time);
					pw.write(group);
					long hashedVal = Murmur3.hash_x86_32(installationId, installationId.length, seed);
					
					BloomFilter<Long> bf = bloomMap.get(customerId);
					if (bf == null) {
						bf = BloomFilter.create(Funnels.longFunnel(), work.config.bfSize, 0.001);
						bloomMap.put(customerId, bf);
					}
					bf.put(hashedVal);

//					if (work.countPerFile > 100_000 && (i % printInterval == 0))
//						System.out.printf("[%s] Done %10d for file: %s\n", new Date().toString(), i, work.path);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				work.gd.update(work.config, bloomMap);
//				if (work.countPerFile > 100_000)
//					System.out.printf("Flushing file: %s in %8.3f ms\n", 
//									  work.path, 
//									  ((double)(System.nanoTime() - start))/1e6);
				closeAll();
			}
		}
	}

	static GeneratorData generatorDataBasedOnSepcifications() {
		GeneratorData gd = new GeneratorData();
		Random rand = new Random();

		IntStream.range(0, 5).forEach(x -> gd.platform.add((short)x));
		IntStream.range(0, 17_000).forEach(x -> gd.orgId.add(UUID.randomUUID()));
		IntStream.range(0, 3).forEach(x -> gd.productName.add((short)x));
		IntStream.range(0, 10).forEach(x -> gd.productVersion.add(x));
//		IntStream.range(0, 10).forEach(x -> gd.customerId.add(Math.abs(rand.nextLong())));
		IntStream.range(0, 100).forEach(x -> gd.customerId.add((long)x));
		IntStream.range(0, 10).forEach(x -> gd.type.add((short)x));
		IntStream.range(0, 3_500_000).forEach(x -> gd.installationId.add(UUID.randomUUID()));
		
		return gd;
	}
	
	private static String getFileName(Date date) {
		return date.toString().replaceAll(":", "-");
	}
	
	public static class BloomFilterFilter implements FileFilter {

		@Override
		public boolean accept(File pathname) {
			return pathname.getName().endsWith(".bloomfilter");
		}
	}
	
	public static String createDirectories(Config config, long customerId, LocalDateInfo ldInfo) {
		StringBuilder sbDirName = new StringBuilder();
		sbDirName.append(config.directory);

		sbDirName.append("/"); sbDirName.append(customerId);  
		Utilities.createDirectory(sbDirName.toString());
		
		sbDirName.append("/"); sbDirName.append(ldInfo.year);  
		Utilities.createDirectory(sbDirName.toString());
		
		sbDirName.append("/"); sbDirName.append(ldInfo.month); 
		Utilities.createDirectory(sbDirName.toString());
		
		sbDirName.append("/"); sbDirName.append(ldInfo.date);  
		Utilities.createDirectory(sbDirName.toString());
		
		Utilities.createDirectory(config.bloomFilterLoc);

		return sbDirName.toString();
	}
	
	@SuppressWarnings("deprecation")
	public static void generateFilesInParallelForDate(
					Utilities.Config config,
					DataSource ds,
					String  tag,
					GeneratorData gd,
					LocalDateInfo ldInfo,
					ThreadPoolExecutor executor) 
					throws IOException, ParseException, InterruptedException, ClassNotFoundException, SQLException {
		
		ConcurrentLinkedQueue<Work> workQueue = new ConcurrentLinkedQueue<>();
		long startTime = System.nanoTime();
		
		// Queue the tasks
		Date startOfDay = new Date(ldInfo.year, ldInfo.month, ldInfo.date); 
		for (int i = 0; i < config.threadPoolSize; i++) {
			long totalMillis = 1000 * 3600 * 24;
			long l = (totalMillis) / (config.threadPoolSize);
			Date startForThis = new Date(startOfDay.getTime() +  + i * l);
			Date endForThis = new Date(startOfDay.getTime() +  + (i + 1) * l);
			
			Work work = new Work(
								getFileName(startForThis),
								i,
								config.countPerFile,
								gd,
								ldInfo,
								startForThis,
								endForThis,
								config);
			workQueue.offer(work);
		}
		long distributeEndTime = System.nanoTime();
		
		// Create threads and sentinels to read from the queues and do work
		CountDownLatch cdl = new CountDownLatch(config.threadPoolSize);
		for (int i = 0; i < config.threadPoolSize; i++) {
			workQueue.add(new Work(null, -1, 0, null, null, null, null, null));
			new Thread(new PerThreadGenerator(workQueue, cdl)).start();
		}
		cdl.await();
		long parquetGenerationTime = System.nanoTime();
		
//		for (Map.Entry<Long, BloomFilter<Long>> me : gd.bfMapGlobal.entrySet()) {
//			System.out.printf("%d:%d ", me.getKey(), me.getValue().approximateElementCount());
//		}
//		System.out.printf("\n");
		
		
		// All files created. Update the Bloom Filters in the database
		Class.forName("com.mysql.jdbc.Driver");
		if (executor != null) {
			updateBloomFilterToDBUsingThreadPoolExecutor(config, ds, ldInfo, gd.bfMapGlobal, executor);
		} else {
			updateBloomFilterToDB(config, ds, ldInfo, gd.bfMapGlobal);
		}
		long cubeBuildTimeTime = System.nanoTime();
				
		// Print statistics
		System.out.printf("[%s] Inserted: %,d records in %,.3f(ms) [Task-Distribute: %,.3f(ms) Parquet Build: %,.3f(ms) Cubing: %,.3f(ms)]\n", 
						  tag, 
						  config.threadPoolSize * config.countPerFile, 
						  ((double)(System.nanoTime() - startTime))/1e6,
						  ((double)(distributeEndTime - startTime))/1e6,
						  ((double)(parquetGenerationTime - distributeEndTime))/1e6,
						  ((double)(cubeBuildTimeTime - parquetGenerationTime))/1e6);
	}
}
