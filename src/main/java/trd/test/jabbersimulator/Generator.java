package trd.test.jabbersimulator;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.xerial.snappy.Snappy;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.sangupta.murmur.Murmur3;

import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;
import trd.test.utilities.Utilities.Config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
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
						ex.printStackTrace();
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

	public static class PerThreadGenerator implements Runnable {
		final CountDownLatch cdl;
		final Work work;
		
		public PerThreadGenerator(Work work, CountDownLatch cdl) {
			this.work = work;
			this.cdl = cdl;
		}
		
		@Override
		public void run() {
			try {
//				generateParquetFile(work);
				generateArrowFile(work);
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				cdl.countDown();
			}
		}
		
		HashMap<Long, ParquetWriter<Group>> parquetWriterMap = new HashMap<>();
		
		@SuppressWarnings("deprecation")
		private synchronized ParquetWriter<Group> getParquetWriter(
														Configuration configuration, 
														MessageType schema, 
														Config conf, 
														String file, 
														long customerId, 
														LocalDateInfo ldInfo, 
														List<Path> addedFilesList) throws IOException {
			ParquetWriter<Group> pw = parquetWriterMap.get(customerId);
			if (pw == null) {
				GroupWriteSupport.setSchema(schema, configuration);

				// Create directories for year, month and date
				String directory = createDirectories(conf, customerId, ldInfo);
				Path fsPath = new Path(directory + "/" + file);
				addedFilesList.add(fsPath);
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

		
		private void closeAllParquet() throws IOException {
			for (Map.Entry<Long, ParquetWriter<Group>> me : parquetWriterMap.entrySet()) {
				if (me.getValue() != null) {
					me.getValue().close();
				}
			}
		}

		@SuppressWarnings({ "deprecation", "unused" })
		private void mergeFiles(List<Path> inputFiles, Configuration config) throws IOException {
			FileMetaData fmd = ParquetFileWriter.mergeMetadataFiles(inputFiles, config).getFileMetaData();
			ParquetFileWriter writer = new ParquetFileWriter(
												config, 
												fmd.getSchema(), 
												new Path("merged.parquet"),
												ParquetFileWriter.Mode.CREATE);
			writer.start();
			for (Path input : inputFiles) {
				writer.appendFile(HadoopInputFile.fromPath(input, config));
			}
			writer.end(fmd.getKeyValueMetaData());
			
			FileSystem fs = FileSystem.get(config);
			fs.delete(new Path("path/to/file"), true);
			for (Path files : inputFiles)
				fs.delete(files);
		}
		
		@SuppressWarnings({ "deprecation", "unused" })
		private void generateParquetFile(Work work) throws IOException, ParseException, SQLException {
			Configuration configuration = new Configuration();
			
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
			List<Path> addedFilesList = new ArrayList<>();
			try {	
				int printInterval = work.countPerFile / 10;
				for (int i = 0; i < work.countPerFile; i++) {
					
					long time = Math.min(work.start.getTime() + i, work.end.getTime());					
					int next = Math.abs(rand.nextInt());
					
					Long customerId = work.gd.customerId.get(next % (work.gd.customerId.size()));
					ParquetWriter<Group> pw = getParquetWriter(configuration, schema, work.config, parquetFile, customerId, work.ldInfo, addedFilesList);					
					
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
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				work.gd.update(work.config, bloomMap);
				closeAllParquet();
//				mergeFiles(addedFilesList, configuration);
			}
		}
		
	    private Schema makeArrowSchema(){
	        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
	        childrenBuilder.add(new Field("platform",       FieldType.nullable(new ArrowType.Int(32, true)), null));
	        childrenBuilder.add(new Field("orgId",          FieldType.nullable(new ArrowType.Binary()), null));
	        childrenBuilder.add(new Field("productName",    FieldType.nullable(new ArrowType.Int(32, true)), null));
	        childrenBuilder.add(new Field("productVersion", FieldType.nullable(new ArrowType.Int(32, true)), null));
	        childrenBuilder.add(new Field("customerid",     FieldType.nullable(new ArrowType.Int(64, true)), null));
	        childrenBuilder.add(new Field("type",           FieldType.nullable(new ArrowType.Int(32, true)), null));
	        childrenBuilder.add(new Field("installationId", FieldType.nullable(new ArrowType.Binary()), null));
	        childrenBuilder.add(new Field("time",           FieldType.nullable(new ArrowType.Int(64, true)), null));
	        return new Schema(childrenBuilder.build(), null);
	    }

		final static String[] fieldNameList = new String[] { 
				"platform",
				"orgId",
				"productName",
				"productVersion",
				"customerid",
				"type",
				"installationId",
				"time"
		};

		public static class ArrowWriterInfo implements Closeable {
			FileOutputStream fos;
			ArrowFileWriter aw;
			VectorSchemaRoot vsr;
			AtomicInteger rowCount = new AtomicInteger(0);
			RootAllocator ra;
			Schema schema;
			
			public void releaseVSR() {
				for (String fieldName : fieldNameList) {
					vsr.getVector(fieldName).clear();
					vsr.getVector(fieldName).close();
				}
				ra.close();
				vsr = null;
				ra = null;
			}

			public void truncateVSR() {
				for (String fieldName : fieldNameList) {
					vsr.getVector(fieldName).clear();
				}
				rowCount.set(0);
				
//		        ra = new RootAllocator(Long.MAX_VALUE);
//				vsr = VectorSchemaRoot.create(schema, ra);
				for (String field: fieldNameList) {
					vsr.getVector(field).setInitialCapacity(1024);
					vsr.getVector(field).allocateNew();
				}

			}
			
			@Override
			public void close() throws IOException {
				aw.writeBatch();
				aw.end();
				aw.close();
				fos.flush();
				fos.close();
				releaseVSR();
			}
		}

		HashMap<Long, ArrowWriterInfo> arrowWriterMap = new HashMap<>();
		private void closeAllArrow() throws IOException {
			for (Map.Entry<Long, ArrowWriterInfo> me : arrowWriterMap.entrySet()) {
				if (me.getValue() != null) {
					me.getValue().close();
				}
			}
		}

		private void updateValueCounts() throws IOException {
			for (Map.Entry<Long, ArrowWriterInfo> me : arrowWriterMap.entrySet()) {
				VectorSchemaRoot vsr = me.getValue().vsr;
				int count = me.getValue().rowCount.get();
				for (String fieldName : fieldNameList) {
					vsr.getVector(fieldName).setValueCount(count);
				}
				vsr.setRowCount(count);
//				System.out.printf("Customer: %d:%d\n", me.getKey(), me.getValue().rowCount.get());
			}
		}

		private void reallocateMemory(ArrowWriterInfo awi) throws IOException {
			updateValueCounts();
			awi.aw.writeBatch();
			awi.truncateVSR();
		}
		
		@SuppressWarnings({ "deprecation" })
		private synchronized ArrowWriterInfo getArrowWriter(
														Schema schema,
														Config conf, 
														String file, 
														long customerId, 
														LocalDateInfo ldInfo,
														int estimatedRowCount,
														List<Path> addedFilesList) throws IOException {
			ArrowWriterInfo awi = arrowWriterMap.get(customerId);
			if (awi == null) {

				// Create directories for year, month and date
				String directory = createDirectories(conf, customerId, ldInfo);
				Path fsPath = new Path(directory + "/" + file);
				
				awi = new ArrowWriterInfo();
		        awi.ra = new RootAllocator(Long.MAX_VALUE);
				awi.fos = new FileOutputStream(fsPath.toString());
		        awi.schema = schema;
		        
				DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
				awi.vsr = VectorSchemaRoot.create(schema, awi.ra);
				awi.aw = new ArrowFileWriter(awi.vsr, provider, awi.fos.getChannel());
				
				for (String field: fieldNameList) {
					awi.vsr.getVector(field).setInitialCapacity(estimatedRowCount);
					awi.vsr.getVector(field).allocateNew();
				}
				awi.aw.start();
				
				addedFilesList.add(fsPath);
				arrowWriterMap.put(customerId, awi);				
			}
			return awi;
		}
		

		@SuppressWarnings("unused")
		private void generateArrowFile(Work work) throws IOException, ParseException, SQLException {
			Configuration configuration = new Configuration();
			
			String arrowFile = work.file + ".arrow";
			Schema schema = makeArrowSchema();

			long start = System.nanoTime();
			Map<Long, BloomFilter<Long>> bloomMap = new HashMap<>();
			Random rand = new Random();
			List<Path> addedFilesList = new ArrayList<>();

			try {	
//				work.countPerFile = 1024 * 512;
				int printInterval = work.countPerFile;

				for (int i = 0; i < work.countPerFile; i++) {
					
					long time = Math.min(work.start.getTime() + i, work.end.getTime());					
					int  next = Math.abs(rand.nextInt());
					
					int estimatedRowCount = work.countPerFile / 100;
					Long customerId = work.gd.customerId.get(next % (work.gd.customerId.size()));					
					ArrowWriterInfo awi = getArrowWriter(schema, work.config, arrowFile, customerId, work.ldInfo, 1024, addedFilesList);					
					
					UUID installationIdUuid = work.gd.installationId.get(Math.abs(rand.nextInt() % (work.gd.installationId.size() - 1)));
					byte[] installationId = Utilities.getUUIDAsByte(installationIdUuid);

					int idx = awi.rowCount.get();
					
					// Get the Field Vectors and update
					((IntVector)awi.vsr.getVector("platform")).setSafe(idx, work.gd.platform.get(next % (work.gd.platform.size())));
					
					byte[] orgId = Utilities.getUUIDAsByte(work.gd.orgId.get(next % (work.gd.orgId.size())));
					VarBinaryVector varBinaryVector = (VarBinaryVector)awi.vsr.getVector("orgId");
	                varBinaryVector.setIndexDefined(idx);
	                varBinaryVector.setValueLengthSafe(idx, orgId.length);					
	                varBinaryVector.setSafe(idx, orgId);
					
					((IntVector)awi.vsr.getVector("productName")).setSafe(idx, work.gd.productName.get(next % (work.gd.productName.size())));
					((IntVector)awi.vsr.getVector("productVersion")).setSafe(idx, work.gd.productVersion.get(next % (work.gd.productVersion.size())));
					((BigIntVector)awi.vsr.getVector("customerid")).setSafe(idx, customerId);
					((IntVector)awi.vsr.getVector("type")).setSafe(idx, work.gd.type.get(next % (work.gd.type.size())));

					varBinaryVector = (VarBinaryVector)awi.vsr.getVector("installationId");
	                varBinaryVector.setIndexDefined(idx);
	                varBinaryVector.setValueLengthSafe(idx, installationId.length);					
	                varBinaryVector.setSafe(idx, installationId);
	                
					((BigIntVector)awi.vsr.getVector("time")).setSafe(idx, time);

					long hashedVal = Murmur3.hash_x86_32(installationId, installationId.length, seed);
					
					BloomFilter<Long> bf = bloomMap.get(customerId);
					if (bf == null) {
						bf = BloomFilter.create(Funnels.longFunnel(), work.config.bfSize, 0.001);
						bloomMap.put(customerId, bf);
					}					
					bf.put(hashedVal);
					awi.rowCount.incrementAndGet();
					
					if (awi.rowCount.get() >= 4096) {
						reallocateMemory(awi);
					}
				}
				updateValueCounts();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				closeAllArrow();
				work.gd.update(work.config, bloomMap);
				System.gc();
			}
		}

		private void DebugBreak() {
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
		return date.toString().replaceAll(":", "-").replaceAll(" ", "_");
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
		
		long startTime = System.nanoTime();
		
		// Queue the tasks
		CountDownLatch cdl = new CountDownLatch(config.threadPoolSize);
		Date startOfDay = new Date(ldInfo.year, ldInfo.month, ldInfo.date);
		List<String> filesCreated = new ArrayList<>();
		
		for (int i = 0; i < config.threadPoolSize; i++) {
			long totalMillis = 1000 * 3600 * 24;
			long l = (totalMillis) / (config.threadPoolSize);
			Date startForThis = new Date(startOfDay.getTime() +  + i * l);
			Date endForThis = new Date(startOfDay.getTime() +  + (i + 1) * l);
			String fileName = getFileName(startForThis);
			Work work = new Work(
								fileName,
								i,
								config.countPerFile,
								gd,
								ldInfo,
								startForThis,
								endForThis,
								config);
			
			filesCreated.add(fileName);
			executor.execute(new PerThreadGenerator(work, cdl));
		}
		long distributeEndTime = System.nanoTime();
		cdl.await();
		long parquetGenerationTime = System.nanoTime();
		
		
//		for (Map.Entry<Long, BloomFilter<Long>> me : gd.bfMapGlobal.entrySet()) {
//			System.out.printf("%d:%d ", me.getKey(), me.getValue().approximateElementCount());
//		}
//		System.out.printf("\n");
		
		
		// All files created. Update the Bloom Filters in the database
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
