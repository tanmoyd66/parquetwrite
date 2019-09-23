package trd.test.jabbersimulator;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import trd.test.jabbersimulator.Generator.GeneratorData;
import trd.test.jabbersimulator.CountDistinct;
import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;

public class JabberSimulator {

	static String TEST_FILE = "/tmp/v11.parquet";

	public static class Config {
		public int bfSize = 100_000;
		public int dbWriterThreads = 10;
	}
	
	private static Double average(Double[] da) {
		Double ret = 0.0;
		for (Double d : da)
			ret += d;
		return ret / da.length;
	}

	public static void DebugBreak() { ; }

	static {
		org.apache.log4j.BasicConfigurator.configure();
	}
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Config config = new Config();

		String mode = args.length < 1 ? "full" : args[0];
		String directory = args.length < 2 ? TEST_FILE : args[1];
		Integer totalBatches = args.length < 3 ? 1 : Integer.parseInt(args[2]);
		Integer countPerFile = args.length < 4 ? 1024 * 1024 * 10 : 1024 * 128 * Integer.parseInt(args[3]);
		Integer numThreads = args.length < 5 ? 1 : Integer.parseInt(args[4]);
		config.bfSize = args.length < 6 ? 100_000 : Integer.parseInt(args[5]);
		config.dbWriterThreads = args.length < 7 ? 10 : Integer.parseInt(args[6]);

		Logger.getRootLogger().setLevel(Level.WARN);
		Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.WARN);
		Logger.getLogger("com.zaxxer.hikari.HikariConfig").setLevel(Level.WARN);
		
		boolean fGenerate = true, fRetrieve = true;
		if (mode.equalsIgnoreCase("generate")) {
			fGenerate = true;
			fRetrieve = false;
		} else if (mode.equalsIgnoreCase("retrieve")) {
			fGenerate = false;
			fRetrieve = true;
		}

		// Cleanup the root level directory for demo purposes
		if (fGenerate) {
			System.out.printf("Cleaning up data...------------------------------------------\n");
			Utilities.resetDemo(directory);
		}

		// Create the data based on specification
		GeneratorData gd = Generator.generatorDataBasedOnSepcifications();

		// Create a thread-pool to do the tasks
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
		
		// Create connection pool to reduce connection buildup time
		Class.forName("com.mysql.jdbc.Driver");
		HikariConfig hconfig = new HikariConfig();
		HikariDataSource ds;
		hconfig.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/jabber");
		hconfig.setUsername("root");
		hconfig.setPassword("27Network");
		hconfig.addDataSourceProperty("cachePrepStmts", "true");
		hconfig.addDataSourceProperty("prepStmtCacheSize", "250");
		hconfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		ds = new HikariDataSource(hconfig);

		// Get the Local Date Information;
		LocalDateInfo ldInfo = new LocalDateInfo(2019, 9, 22);

		// First generate the data for 10 days
		if (fGenerate) {
			System.out.printf("Creating Data for 90 days...------------------------------------------\n");
			for (int i = 0; i < 90; i++) {
//				System.out.printf("Storage:%s\n", ldInfo.getStorageDirectory());
//				if (ldInfo.getStorageDirectory().startsWith("2019/11/31")) {
//					DebugBreak();
				Generator.generateFilesInParallelForDate(
							ds, 
							ldInfo.toString(), 
							directory, 
							totalBatches,
							countPerFile, 
							numThreads, 
							gd, 
							ldInfo,
							config,
							executor);
//				}
				ldInfo.addDays(1);
				gd.reset();
			}
		}

		if (!fRetrieve)
			return;

		// Now search for count of records that meet a criterion
		LocalDateInfo ldInfoToday = new LocalDateInfo(LocalDate.now());
		LocalDateInfo ldInfoYTD   = new LocalDateInfo(2019, -1, -1);
		LocalDateInfo ldInfoMTD   = new LocalDateInfo(2019, 9, -1);
		LocalDateInfo ldDate1     = new LocalDateInfo(2019, 9, 22);
		LocalDateInfo ldDate2     = new LocalDateInfo(2019, 10,10);
		LocalDateInfo ldDate3     = new LocalDateInfo(2019, 9, 22);
		LocalDateInfo ldDate4     = new LocalDateInfo(2019, 9, 23);

		List<Long> customers = new ArrayList<Long>();
		IntStream.range(0, 10).forEach(x -> customers.add((long) x));

		Tuples.Triple<String, LocalDateInfo, LocalDateInfo>[] ranges = new Tuples.Triple[] {
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Date Range:      ", ldDate1, ldDate2),
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("For a given day: ", ldInfoToday, ldInfoToday),
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Day 1            ", ldDate3, ldDate3),
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Day 2            ", ldDate4, ldDate4),
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Day 1 - Day 2    ", ldDate3, ldDate4),
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Month To Date    ", ldInfoMTD, ldInfoMTD), 
				new Tuples.Triple<String, LocalDateInfo, LocalDateInfo>("Year To Date     ", ldInfoYTD, ldInfoYTD),
		};

		System.out.printf("Retrieving count distincts from database...------------------------------------------\n");
		try (Connection conn = ds.getConnection()) {
			int j = -1;

			for (Tuples.Triple<String, LocalDateInfo, LocalDateInfo> range : ranges) {
				j++;

				Double[] elapsed = new Double[customers.size()];
				StringBuilder sb = new StringBuilder();

				for (int i = 0; i < customers.size(); i++) {
					Long customerId = customers.get(i);

					long startTime = System.nanoTime();
					Long count = CountDistinct.getDistinctCountFromDB(config, customerId, range.b, range.c);
					elapsed[i] = (double) (System.nanoTime() - startTime) / 1e6;

					sb.append(String.format("[%d:%d]", customerId, count));
				}
				System.out.printf("Time to calculate [%s(%s,%s)]\t: %8.3f ms ...%s\n", range.a, range.b.toLiteral(), range.c.toLiteral(), average(elapsed), sb.toString());
			}
		}
		System.out.printf("Retrieving count distincts from files by scanning...------------------------------------------\n");
		int j = 0;
		for (Tuples.Triple<String, LocalDateInfo, LocalDateInfo> range : ranges) {
			j++;

			Double[] elapsed = new Double[customers.size()];
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < customers.size(); i++) {
				Long customerId = customers.get(i);

				if (range.b == ldInfoYTD)
					DebugBreak();
				long startTime = System.nanoTime();
				Long count = CountDistinctByScan.getDistinctCountFromFile(directory, config, customerId, executor, range.b, range.c);
				elapsed[i] = (double) (System.nanoTime() - startTime) / 1e6;

				sb.append(String.format("[%d:%d]", customerId, count));
			}
			System.out.printf("Time to calculate [%s(%s,%s)]\t: %8.3f ms ...%s\n", range.a, range.b.toLiteral(), range.c.toLiteral(), average(elapsed), sb.toString());
		}
		System.out.printf("Done...------------------------------------------\n");
		executor.shutdown();
	}
}
