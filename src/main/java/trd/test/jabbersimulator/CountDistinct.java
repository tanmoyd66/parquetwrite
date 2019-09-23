package trd.test.jabbersimulator;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import trd.test.jabbersimulator.Generator;
import trd.test.jabbersimulator.JabberSimulator.Config;
import trd.test.utilities.LocalDateInfo;
import trd.test.utilities.Tuples;
import trd.test.utilities.Utilities;

public class CountDistinct {
	
	public static long countFromBloomFilters(List<BloomFilter<Long>> bfList) {
		if (bfList.size() == 0)
			return 0;
		
		BloomFilter<Long> bfTotal = BloomFilter.create(Funnels.longFunnel(), 1_000_000, 0.001);
		for (BloomFilter<Long> bf: bfList) {
			bfTotal.putAll(bf);
		}
		return bfTotal.approximateElementCount();
	}
	
	public static long getDirectCountForADay(Config config, long customerId, LocalDateInfo date, Connection conn) throws SQLException {
		String sqlSelectFmt = "select count from countdistinctoninstanceid where customerid = %d and year = %d and month = %d and day = %d;";
		String sql = String.format(sqlSelectFmt, customerId, date.year, date.month, date.date);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		if (rs != null) {
			while (rs.next()) {
				return rs.getInt(1);
			}
		}
		return 0;
	}

	public static long getCountsForDays(Config config, long customerId, int year, List<Tuples.Pair<Integer, Integer>> dates, Connection conn) throws SQLException, IOException {
		if (dates.size() == 0)
			return 0;
		
		StringBuilder sb = new StringBuilder();
		boolean fFirstTime = true;
		for (Tuples.Pair<Integer, Integer> date : dates) {
			if (!fFirstTime)
				sb.append(" or ");
			else 
				fFirstTime = false;
			sb.append(String.format("(month = %d and day = %d)", date.a, date.b)); 
		}
		
		String sqlSelectFmt = "select countdata from countdistinctoninstanceid where customerid = %d and year = %d and (%s)";
		String sql = String.format(sqlSelectFmt, customerId, year, sb.toString());
		ResultSet rs = conn.createStatement().executeQuery(sql);
		if (rs != null) {
			BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), config.bfSize, 0.001);
			while (rs.next()) {
				BloomFilter<Long> bfThis = Generator.bytesToBf(rs.getBytes(1));
				bf.putAll(bfThis);
			}
			return (int) bf.approximateElementCount();
		}
		return 0;
	}
	
	public static long getDistinctCountFromDB(Config config, long customerId, LocalDateInfo start, LocalDateInfo end) throws SQLException, IOException {
		if (start.year != end.year)
			throw new InvalidParameterException("Cannot aggregate count distincts over years");
		if (start.compareTo(end) > 0)
			throw new InvalidParameterException("End date cannot be greater than start");
		else if (start.compareTo(end) == 0) {
			try (Connection conn = Utilities.getConnection()) {
				return getDirectCountForADay(config, customerId, start, conn);
			}
		} else {
			List<Tuples.Pair<Integer, Integer>> probeList = new ArrayList<>();
			if (start.month == end.month) {
				// Month is same. Add days for this month
				for (int i = start.date; i <= end.date; i++) {
					probeList.add(new Tuples.Pair<Integer, Integer>(start.month, i));
				}
			} else {
				// Add the remaining days of the start month
				for (int i = start.date; i <= 31; i++) {
					probeList.add(new Tuples.Pair<Integer, Integer>(start.month, i));
				}
				
				// Add full months if needed
				for (int i = start.month + 1; i < end.month; i++) {
					probeList.add(new Tuples.Pair<Integer, Integer>(i, -1));
				}
				
				// Add the remaining days of the end month
				for (int i = 1; i <= end.date; i++) {
					probeList.add(new Tuples.Pair<Integer, Integer>(end.month, i));
				}
			}
			try (Connection conn = Utilities.getConnection()) {
				return getCountsForDays(config, customerId, start.year, probeList, conn);
			}
		}
	}
	
	
}
