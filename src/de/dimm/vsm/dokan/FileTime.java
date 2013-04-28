/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.dokan;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 *
 * @author Administrator
 */
public class FileTime
{

    private static final long MILLISECONDS_IN_SECOND = 1000;
    private static final long MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final long MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final long MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    // the difference between January 1, 1970, 00:00:00 GMT (Java begin date) and January 1, 1601 (FILETIME begin date)
    private static long TIME_DIFFERENCE_IN_MILLISECONDS = 11644473600000L;

    private FileTime()
    {
    }

    /**
     * Converts the value of this <code>FILETIME</code> structure to Java {@link Date} format.
     *
     * @return the converted value
     */
    static public Date calctoDate( long ftime )
    {
        //calculate number of 100-nanosecond intervals since January 1, 1601

        long numberOf100NanosecondIntervals = ftime;

        long milliseconds = numberOf100NanosecondIntervals / 10000;

        long days = milliseconds / MILLISECONDS_IN_DAY;
        milliseconds -= days * MILLISECONDS_IN_DAY;
        long hours = milliseconds / MILLISECONDS_IN_HOUR;
        milliseconds -= hours * MILLISECONDS_IN_HOUR;
        long minutes = milliseconds / MILLISECONDS_IN_MINUTE;
        milliseconds -= minutes * MILLISECONDS_IN_MINUTE;
        long seconds = milliseconds / MILLISECONDS_IN_SECOND;
        milliseconds -= seconds * MILLISECONDS_IN_SECOND;

        // since January 1, 1601
        Calendar calendar = getGMTCalendar();
        calendar.set(1601, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        calendar.add(Calendar.DATE, (int) days);
        calendar.add(Calendar.HOUR_OF_DAY, (int) hours);
        calendar.add(Calendar.MINUTE, (int) minutes);
        calendar.add(Calendar.SECOND, (int) seconds);
        calendar.add(Calendar.MILLISECOND, (int) milliseconds);

        return calendar.getTime();
    }

    private static Calendar getGMTCalendar()
    {
        return Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    }

    /**
     * Converts the specified {@link Date} value to the internal presentation of <code>FILETIME</code> format.
     *
     * @param date the date to set
     */
    static public long fromDate( Date date )
    {
        long milliseconds = date.getTime() + TIME_DIFFERENCE_IN_MILLISECONDS;
        long numberOf100NanosecondIntervals = milliseconds * 10000;

        return numberOf100NanosecondIntervals;
    }
    static public long toWinFileTime( long java_time )
    {
        long milliseconds = java_time + TIME_DIFFERENCE_IN_MILLISECONDS;
        long numberOf100NanosecondIntervals = milliseconds * 10000;

        return numberOf100NanosecondIntervals;
    }

    static public Date toDate( long ftime )
    {
        long milliseconds = ftime / (10000) - TIME_DIFFERENCE_IN_MILLISECONDS;

        if (milliseconds < 0)
            milliseconds = 0;

        return new Date(milliseconds);
    }

    static public long toJavaTime( long ftime )
    {
        long milliseconds = ftime / (10000) - TIME_DIFFERENCE_IN_MILLISECONDS;

        if (milliseconds < 0)
            milliseconds = 0;

        return milliseconds;
    }
}
