package trd.test.utilities;

import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class LocalDateInfo implements Comparable<LocalDateInfo> {
	public int year;
	public int month;
	public int date;

	public LocalDateInfo(int year, int month, int date) {
		this.date = date;
		this.month = month;
		this.year = year;
	}

	public LocalDateInfo(String stringDate) {
		this(java.sql.Date.valueOf(stringDate));
	}

	public LocalDateInfo(java.sql.Date sqlDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(sqlDate);
		this.date = c.get(Calendar.DATE);
		this.month = c.get(Calendar.MONTH) + 1;
		this.year = c.get(Calendar.YEAR);
	}

	public LocalDateInfo(Date javaDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(javaDate);
		this.date = c.get(Calendar.DATE);
		this.month = c.get(Calendar.MONTH) + 1;
		this.year = c.get(Calendar.YEAR);
	}

	public LocalDateInfo(LocalDate ld) {
		this.year = ld.getYear();
		this.month = ld.getMonthValue();
		this.date = ld.getDayOfMonth();
	}

	public static LocalDateInfo of(int year, Month month, int dayOfMonth) {
		if (dayOfMonth == -1) {
			return new LocalDateInfo(year, month.getValue(), dayOfMonth);
		}
		return new LocalDateInfo(LocalDate.of(year, month, dayOfMonth));
	}

	public static LocalDateInfo of(int year, int month, int dayOfMonth) {
		return new LocalDateInfo(year, month, dayOfMonth);
	}
	
	@SuppressWarnings("deprecation")
	public void addDays(int days) {
		java.sql.Date thisDate = new java.sql.Date(year, month, date);
		LocalDate ld = LocalDate.of(year, month, date);
		
		Calendar c = Calendar.getInstance();
		c.set(ld.getYear(), ld.getMonthValue() - 1, ld.getDayOfMonth());
		c.add(Calendar.DATE, days);
		
		LocalDate thatDate = LocalDate.of(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DATE));
		this.year = thatDate.getYear();
		this.month = thatDate.getMonth().getValue();
		this.date = thatDate.getDayOfMonth();
	}

	public String toString() {
		Calendar c = Calendar.getInstance();
		c.set(year, month - 1, date);
		return new java.sql.Date(c.getTimeInMillis()).toString();
	}
	public String toLiteral() {
		return String.format("%d/%d/%d", year, month, date);
	}

	public String getStorageDirectory() {
		return String.format("%d/%d/%d", year, month, date);
	}

	@Override
	public int compareTo(LocalDateInfo o) {
		if (this.year == o.year) {
			if (this.month == o.month) {
				return new Integer(this.date).compareTo(o.date);
			} else {
				return new Integer(this.month).compareTo(o.month);
			}
		} else {
			return new Integer(this.year).compareTo(o.year);
		}
	}

	public static List<String> getAllDirectoriesBetween2Dates(LocalDateInfo startDate, LocalDateInfo endDate) {
		if ((startDate.month == -1 || startDate.date == -1) && 
			startDate.compareTo(endDate) != 0)
			throw new InvalidParameterException("Invalid dates");
		
		LocalDate _startDate = null, _endDate = null;		
		if (startDate.compareTo(endDate) == 0) {
			if (startDate.month == -1) {
				_startDate = LocalDate.of(startDate.year, Month.JANUARY, 1);
				_endDate   = LocalDate.now();
			} else if (startDate.date == -1) {
				_startDate = LocalDate.of(startDate.year, startDate.month, 1);
				_endDate   = LocalDate.now();
			}
		} else if (startDate.compareTo(endDate) > 0) {
			throw new InvalidParameterException("Invalid dates");
		} else {
			_startDate = LocalDate.of(startDate.year, startDate.month, startDate.date);
			_endDate   = LocalDate.of(endDate.year, endDate.month, endDate.date);
		}
		
		long numOfDaysBetween = ChronoUnit.DAYS.between(_startDate, _endDate);
		LocalDate dummyStartDate = _startDate;
		return IntStream
				.iterate(0, i -> i + 1)
				.limit(numOfDaysBetween)
				.mapToObj(i -> dummyStartDate.plusDays(i))
				.map((LocalDate ld) -> { return new LocalDateInfo(ld); })
				.map((LocalDateInfo x) -> x.getStorageDirectory())
				.collect(Collectors.toList());
	}

	public static List<LocalDateInfo> getAllLocalDateInfosBetween2Dates(LocalDateInfo startDate, LocalDateInfo endDate) {
		if ((startDate.month == -1 || startDate.date == -1) && 
				startDate.compareTo(endDate) != 0)
				throw new InvalidParameterException("Invalid dates");

		LocalDate _startDate = null, _endDate = null;		
		if (startDate.compareTo(endDate) == 0) {
			if (startDate.month == -1) {
				_startDate = LocalDate.of(startDate.year, Month.JANUARY, 1);
				_endDate   = LocalDate.now();
			} else if (startDate.date == -1) {
				_startDate = LocalDate.of(startDate.year, startDate.month, 1);
				_endDate   = LocalDate.now();
			}
		} else if (startDate.compareTo(endDate) > 0) {
			throw new InvalidParameterException("Invalid dates");
		} else {
			_startDate = LocalDate.of(startDate.year, startDate.month, startDate.date);
			_endDate   = LocalDate.of(endDate.year, endDate.month, endDate.date);
		}
		
		long numOfDaysBetween = ChronoUnit.DAYS.between(_startDate, _endDate);
		LocalDate dummyStartDate = _startDate;
		return IntStream
				.iterate(0, i -> i + 1)
				.limit(numOfDaysBetween)
				.mapToObj(i -> dummyStartDate.plusDays(i))
				.map((LocalDate ld) -> { return new LocalDateInfo(ld); })
				.collect(Collectors.toList());
	}

	
	public static void main(String[] args) {
		System.out.printf("%s\n", new LocalDateInfo("2019-08-15").getStorageDirectory());
		System.out.printf("%s\n", new LocalDateInfo("2019-01-15").getStorageDirectory());
		
		System.out.printf("%s\n", getAllDirectoriesBetween2Dates(
								LocalDateInfo.of(2019, Month.SEPTEMBER, 15), 
								LocalDateInfo.of(2019, Month.DECEMBER, 15)));

		System.out.printf("%s\n", getAllDirectoriesBetween2Dates(
				LocalDateInfo.of(2019, Month.SEPTEMBER, -1), 
				LocalDateInfo.of(2019, Month.SEPTEMBER, -1)));

		System.out.printf("%s\n", getAllDirectoriesBetween2Dates(
				LocalDateInfo.of(2019, -1, -1), 
				LocalDateInfo.of(2019, -1, -1)));
	}
}
