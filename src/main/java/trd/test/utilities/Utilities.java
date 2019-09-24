package trd.test.utilities;

import java.io.File;
import java.io.FileFilter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

public class Utilities {
	static String TEST_FILE = "/tmp/v11.parquet";

	public static class Config {
		public int      bfSize = 100_000;
		public String   mode = "full";
		public String   directory = TEST_FILE;
		public String   bloomFilterLoc = TEST_FILE + "/bloomfilters";
		public Integer  totalBatches = 1;
		public Integer  countPerFile = 1024 * 1024 * 10;
		public Integer  threadPoolSize = 10;

		public void fillFromOptions(String[] args) {
			mode            = args.length >= 1 ? args[0] : mode;
			directory       = args.length >= 2 ? args[1] : directory;
			bloomFilterLoc  = args.length >= 2 ? args[1] + "/bloomfilters" : bloomFilterLoc;
			totalBatches    = args.length >= 3 ? Integer.parseInt(args[2]) : totalBatches;
			countPerFile    = args.length >= 4 ? 1024 * 128 * Integer.parseInt(args[3]) : countPerFile;
			threadPoolSize  = args.length >= 5 ? Integer.parseInt(args[4]) : threadPoolSize;
			bfSize          = args.length >= 6 ? Integer.parseInt(args[5]) : bfSize;
		}
	}

	private static boolean deleteDirectory(File dir) {
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDirectory(children[i]);
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	public static File[] listFiles(File dir) {
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			return children;
		}
		return null;
	}

	public static File[] listFiles(File dir, FileFilter filter) {
		if (dir.isDirectory()) {
			File[] children = dir.listFiles(filter);
			return children;
		}
		return null;
	}

	public static void deleteDirectory(String dirPath) {
		File dir = new File(dirPath);
		deleteDirectory(dir);
	}

	public static void createDirectory(String dirPath) {
		File dir = new File(dirPath);
		if (!dir.exists())
			dir.mkdirs();
	}

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/jabber?user=root&password=27Network");
	}

	public static void resetDemo(Config config) {
		deleteDirectory(config.directory);
		try (Connection c = getConnection()) {
			c.createStatement().execute("truncate table countdistinctoninstanceid");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

    @SuppressWarnings("rawtypes")
	public static class ConnectionPool {
		 
	    // JDBC Driver Name & Database URL
	    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	    static final String JDBC_DB_URL = "jdbc:mysql://127.0.0.1:3306/jabber";
	 
	    // JDBC Database Credentials
	    static final String JDBC_USER = "root";
	    static final String JDBC_PASS = "27Network";
	 
		private static GenericObjectPool gPool = null;
	 
	    @SuppressWarnings("unused")
	    public DataSource setUpPool() throws Exception {

	    	Class.forName(JDBC_DRIVER);
	        gPool = new GenericObjectPool();
	        gPool.setMaxActive(16);
	 
	        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
	 
	        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
	        return new PoolingDataSource(gPool);
	    }
	 
	    public GenericObjectPool getConnectionPool() {
	        return gPool;
	    }
	 
	    public void printDbStatus() {
	        System.out.println("Max.: " + getConnectionPool().getMaxActive() + "; Active: " + getConnectionPool().getNumActive() + "; Idle: " + getConnectionPool().getNumIdle());
	    }
	 
	}
	
	public static byte[] getUUIDAsByte(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}

}
