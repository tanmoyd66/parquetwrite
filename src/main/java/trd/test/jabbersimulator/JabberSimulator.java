package trd.test.jabbersimulator;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
		
		// Configure logging. Important as parquet performance depends on this
		Logger.getRootLogger().setLevel(Level.WARN);
		Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.WARN);
		Logger.getLogger("com.zaxxer.hikari.HikariConfig").setLevel(Level.WARN);
		
		// Initialize configuration 
		Utilities.Config config = new Utilities.Config();
		config.fillFromOptions(args);
		
		// Set Execution mode
		boolean fGenerate = true, fRetrieve = true;
		if (config.mode.equalsIgnoreCase("generate")) {
			fGenerate = true;
			fRetrieve = false;
		} else if (config.mode.equalsIgnoreCase("retrieve")) {
			fGenerate = false;
			fRetrieve = true;
		}

		// Cleanup the root level directory for demo purposes
		if (fGenerate) {
			System.out.printf("Cleaning up data...------------------------------------------\n");
			Utilities.resetDemo(config);
		}

		// Create the data based on specification
		GeneratorData gd = Generator.generatorDataBasedOnSepcifications();

		// Create a thread-pool to do the tasks
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.threadPoolSize);
		
		// Create connection pool to reduce connection buildup time
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

		// First generate the data for 90 days
		if (fGenerate) {
			System.out.printf("Creating Data for 90 days...------------------------------------------\n");
			System.out.println(new Date());
			for (int i = 0; i < 90; i++) {
				Generator.generateFilesInParallelForDate(
							config,
							ds, 
							ldInfo.toString(), 
							gd, 
							ldInfo,
							executor);
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
		LocalDateInfo ldDate1     = new LocalDateInfo(2019, 9, 24);
		LocalDateInfo ldDate2     = new LocalDateInfo(2019, 10,10);
		LocalDateInfo ldDate3     = new LocalDateInfo(2019, 9, 24);
		LocalDateInfo ldDate4     = new LocalDateInfo(2019, 9, 25);

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
		System.out.println(new Date());
		try (Connection conn = ds.getConnection()) {
			for (Tuples.Triple<String, LocalDateInfo, LocalDateInfo> range : ranges) {

				Double[] elapsed = new Double[customers.size()];
				StringBuilder sb = new StringBuilder();

				for (int i = 0; i < customers.size(); i++) {
					Long customerId = customers.get(i);

					long startTime = System.nanoTime();
					Long count = CountDistinct.getDistinctCountFromDB(config, customerId, range.b, range.c);
					elapsed[i] = (double) (System.nanoTime() - startTime) / 1e6;

					sb.append(String.format("[%d:%d]", customerId, count));
				}
				System.out.printf("Time to calculate [%s(%s,%s)]\t: %8.3f ms ...%s\n", range.a, range.b.toLiteral(),
						range.c.toLiteral(), average(elapsed), sb.toString());
			}
		}

		ThreadPoolExecutor executor2 = executor; //(ThreadPoolExecutor) Executors.newFixedThreadPool(2);
//		customers.clear();
//		IntStream.range(0, 1).forEach(x -> customers.add((long) x));
		System.out.printf("Retrieving count distincts from files by scanning...------------------------------------------\n");
		System.out.println(new Date());
		for (Tuples.Triple<String, LocalDateInfo, LocalDateInfo> range : ranges) {

			Double[] elapsed = new Double[customers.size()];
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < customers.size(); i++) {
				Long customerId = customers.get(i);

				if (range.b == ldInfoYTD)
					DebugBreak();
				long startTime = System.nanoTime();
				Long count = CountDistinctByScan.getDistinctCountFromFile(config, customerId, executor2, range.b, range.c);
				elapsed[i] = (double) (System.nanoTime() - startTime) / 1e6;

				sb.append(String.format("[%d:%d]", customerId, count));
			}
			System.out.printf("Time to calculate [%s(%s,%s)]\t: %8.3f ms ...%s\n", range.a, range.b.toLiteral(), range.c.toLiteral(), average(elapsed), sb.toString());
		}
		System.out.printf("Done...------------------------------------------\n");
		executor.shutdown();
		executor2.shutdown();
		System.out.println(new Date());
	}
}
