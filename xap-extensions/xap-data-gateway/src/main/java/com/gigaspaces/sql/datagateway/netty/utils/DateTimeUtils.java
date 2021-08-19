/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.sql.datagateway.netty.exception.NonBreakingException;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.chrono.IsoEra;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.function.Supplier;

public class DateTimeUtils {
    private static final int MAX_NANOS_BEFORE_WRAP_ON_ROUND = 999999500;
    private static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
    private static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
    private static final char[] ZEROS = {'0', '0', '0', '0', '0', '0', '0', '0', '0'};
    private static final char[][] NUMBERS;
    private static final Duration ONE_MICROSECOND = Duration.ofNanos(1000);
    private static final LocalTime MAX_LOCAL_TIME = LocalTime.MAX.minus(Duration.ofNanos(500));
    private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(4713, 1, 1).with(ChronoField.ERA, IsoEra.BCE.getValue());
    private static final LocalDateTime MIN_LOCAL_DATETIME = MIN_LOCAL_DATE.atStartOfDay();
    private static final LocalDateTime MAX_LOCAL_DATETIME = LocalDateTime.MAX.minus(Duration.ofMillis(500));

    static {
        // The expected maximum value is 60 (seconds), so 64 is used "just in case"
        NUMBERS = new char[64][];
        for (int i = 0; i < NUMBERS.length; i++) {
            NUMBERS[i] = ((i < 10 ? "0" : "") + i).toCharArray();
        }
    }

    private final Calendar sessionCalendar = new GregorianCalendar();
    private final StringBuilder sbuf = new StringBuilder();
    private final Supplier<TimeZone> tz;

    private Calendar tmpCalendar;
    private int tmpOffset;

    public DateTimeUtils(Supplier<TimeZone> tz) {
        this.tz = tz;
    }

    public static String convertTimeZone(String tz) {
        if (tz == null || tz.length() < 4)
            return tz;
        switch (tz.substring(0, 4).toUpperCase()) {
            case "GMT+":
                return "GMT-" + tz.substring(4);
            case "GMT-":
                return "GMT+" + tz.substring(4);
            case "UTC+":
                return "UTC-" + tz.substring(4);
            case "UTC-":
                return "UTC+" + tz.substring(4);
            default:
                return tz;
        }
    }

    private Calendar setupCalendar() {
        sessionCalendar.setTimeZone(getTimeZone());
        return sessionCalendar;
    }

    TimeZone getTimeZone() {
        return tz.get();
    }

    private static boolean nanosExceed499(int nanos) {
        return nanos % 1000 > 499;
    }

    public String toString(Object x) throws ProtocolException {
        return toString(x, true);
    }

    public String toString(Object x, boolean withTimeZone) throws ProtocolException {
        if (x == null)
            return "NULL";

        if (x instanceof java.util.Date) {
            return toString((java.util.Date) x, withTimeZone);
        }

        if (!withTimeZone) {
            if (x instanceof LocalDate) {
                return toString((LocalDate) x);
            }
            if (x instanceof LocalTime) {
                return toString((LocalTime) x);
            }
            if (x instanceof LocalDateTime) {
                return toString((LocalDateTime) x);
            }
        } else {
            if (x instanceof OffsetDateTime) {
                return toString((OffsetDateTime) x);
            }
            if (x instanceof OffsetTime) {
                return toString((OffsetTime) x);
            }
            if (x instanceof Instant) {
                return toString((Instant) x);
            }
            if (x instanceof Calendar) {
                return toString((Calendar) x);
            }
        }

        throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE, "Unsupported object type: " + x.getClass());
    }

    public String toString(java.util.Date x) {
        return toString(x, true);
    }

    public String toString(java.util.Date x, boolean withTimeZone) {
        if (x instanceof Timestamp) {
            return toString((Timestamp) x, withTimeZone);
        }
        if (x instanceof Time) {
            return toString((Time) x, withTimeZone);
        }
        if (x instanceof Date) {
            return toString((Date) x, withTimeZone);
        }

        return toString(new Timestamp(x.getTime()), withTimeZone);
    }

    public String toString(Timestamp x) {
        return toString(x, true);
    }

    public String toString(Timestamp x, boolean withTimeZone) {
        if (x.getTime() == DATE_POSITIVE_INFINITY) {
            return "infinity";
        } else if (x.getTime() == DATE_NEGATIVE_INFINITY) {
            return "-infinity";
        }

        Calendar cal = setupCalendar();
        long timeMillis = x.getTime();

        // Round to microseconds
        int nanos = x.getNanos();
        if (nanos >= MAX_NANOS_BEFORE_WRAP_ON_ROUND) {
            nanos = 0;
            timeMillis++;
        } else if (nanosExceed499(nanos)) {
            // PostgreSQL does not support nanosecond resolution yet, and appendTime will just ignore
            // 0..999 part of the nanoseconds, however we subtract nanos % 1000 to make the value
            // a little bit saner for debugging reasons
            nanos += 1000 - nanos % 1000;
        }
        cal.setTimeInMillis(timeMillis);

        sbuf.setLength(0);

        appendDate(sbuf, cal);
        sbuf.append(' ');
        appendTime(sbuf, cal, nanos);
        if (withTimeZone) {
            appendTimeZone(sbuf, cal);
        }
        appendEra(sbuf, cal);

        return sbuf.toString();
    }

    public String toString(Date x) {
        return toString(x, true);
    }

    public String toString(Date x, boolean withTimeZone) {
        if (x.getTime() == DATE_POSITIVE_INFINITY) {
            return "infinity";
        } else if (x.getTime() == DATE_NEGATIVE_INFINITY) {
            return "-infinity";
        }

        Calendar cal = setupCalendar();
        cal.setTime(x);

        sbuf.setLength(0);

        appendDate(sbuf, cal);
        appendEra(sbuf, cal);
        if (withTimeZone) {
            sbuf.append(' ');
            appendTimeZone(sbuf, cal);
        }

        return sbuf.toString();
    }

    public String toString(Time x) {
        return toString(x, true);
    }

    public String toString(Time x, boolean withTimeZone) {
        Calendar cal = setupCalendar();
        cal.setTime(x);

        sbuf.setLength(0);

        appendTime(sbuf, cal, cal.get(Calendar.MILLISECOND) * 1000000);

        if (withTimeZone) {
            appendTimeZone(sbuf, cal);
        }

        return sbuf.toString();
    }

    public String toString(LocalDate localDate) {
        if (LocalDate.MAX.equals(localDate)) {
            return "infinity";
        } else if (localDate.isBefore(MIN_LOCAL_DATE)) {
            return "-infinity";
        }

        sbuf.setLength(0);

        appendDate(sbuf, localDate);
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(LocalTime localTime) {
        sbuf.setLength(0);

        if (localTime.isAfter(MAX_LOCAL_TIME)) {
            appendTime(sbuf, 24, 0, 0, 0);
        } else {
            int nano = localTime.getNano();
            if (nanosExceed499(nano)) {
                // Technically speaking this is not a proper rounding, however
                // it relies on the fact that appendTime just truncates 000..999 nanosecond part
                localTime = localTime.plus(ONE_MICROSECOND);
            }
            appendTime(sbuf, localTime);
        }

        return sbuf.toString();
    }

    public String toString(LocalDateTime localDateTime) {
        sbuf.setLength(0);

        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = localDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            localDateTime = localDateTime.plus(ONE_MICROSECOND);
        }
        LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(OffsetDateTime offsetDateTime) {
        sbuf.setLength(0);

        LocalDateTime localDateTime = offsetDateTime.toLocalDateTime();
        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = offsetDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            offsetDateTime = offsetDateTime.plus(ONE_MICROSECOND);
        }
        LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendTimeZone(sbuf, offsetDateTime.getOffset());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(ZonedDateTime zonedDateTime) {
        sbuf.setLength(0);

        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = zonedDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            zonedDateTime = zonedDateTime.plus(ONE_MICROSECOND);
        }
        LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendTimeZone(sbuf, zonedDateTime.getOffset());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(OffsetTime offsetTime) {
        sbuf.setLength(0);

        LocalTime localTime = offsetTime.toLocalTime();
        if (localTime.isAfter(MAX_LOCAL_TIME)) {
            appendTime(sbuf, 24, 0, 0, 0);
        } else {
            int nano = offsetTime.getNano();
            if (nanosExceed499(nano)) {
                // Technically speaking this is not a proper rounding, however
                // it relies on the fact that appendTime just truncates 000..999 nanosecond part
                offsetTime = offsetTime.plus(ONE_MICROSECOND);
            }
            appendTime(sbuf, localTime);
        }
        appendTimeZone(sbuf, offsetTime.getOffset());

        return sbuf.toString();
    }

    public String toString(Instant instant) {
        return toString(instant.atZone(getTimeZone().toZoneId()));
    }

    public String toString(Calendar calendar) {
        sbuf.setLength(0);

        appendDate(sbuf, calendar);
        sbuf.append(' ');
        appendTime(sbuf, calendar, calendar.get(Calendar.MILLISECOND) * 1000000);
        appendTimeZone(sbuf, calendar);
        appendEra(sbuf, calendar);

        return sbuf.toString();
    }

    private static void appendEra(StringBuilder sb, Calendar cal) {
        if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
            sb.append(" BC");
        }
    }

    private static void appendEra(StringBuilder sb, LocalDate localDate) {
        if (localDate.get(ChronoField.ERA) == IsoEra.BCE.getValue()) {
            sb.append(" BC");
        }
    }

    private static void appendDate(StringBuilder sb, Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        appendDate(sb, year, month, day);
    }

    private static void appendDate(StringBuilder sb, int year, int month, int day) {
        // always use at least four digits for the year so very
        // early years, like 2, don't get misinterpreted
        //
        int prevLength = sb.length();
        sb.append(year);
        int leadingZerosForYear = 4 - (sb.length() - prevLength);
        if (leadingZerosForYear > 0) {
            sb.insert(prevLength, ZEROS, 0, leadingZerosForYear);
        }

        sb.append('-');
        sb.append(NUMBERS[month]);
        sb.append('-');
        sb.append(NUMBERS[day]);
    }

    private static void appendDate(StringBuilder sb, LocalDate localDate) {
        int year = localDate.get(ChronoField.YEAR_OF_ERA);
        int month = localDate.getMonthValue();
        int day = localDate.getDayOfMonth();
        appendDate(sb, year, month, day);
    }

    private static void appendTime(StringBuilder sb, Calendar cal, int nanos) {
        int hours = cal.get(Calendar.HOUR_OF_DAY);
        int minutes = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        appendTime(sb, hours, minutes, seconds, nanos);
    }

    private static void appendTime(StringBuilder sb, int hours, int minutes, int seconds, int nanos) {
        sb.append(NUMBERS[hours]);

        sb.append(':');
        sb.append(NUMBERS[minutes]);

        sb.append(':');
        sb.append(NUMBERS[seconds]);

        // Add nanoseconds.
        // This won't work for server versions < 7.2 which only want
        // a two digit fractional second, but we don't need to support 7.1
        // anymore and getting the version number here is difficult.
        //
        if (nanos < 1000) {
            return;
        }
        sb.append('.');
        int len = sb.length();
        sb.append(nanos / 1000); // append microseconds
        int needZeros = 6 - (sb.length() - len);
        if (needZeros > 0) {
            sb.insert(len, ZEROS, 0, needZeros);
        }

        int end = sb.length() - 1;
        while (sb.charAt(end) == '0') {
            sb.deleteCharAt(end);
            end--;
        }
    }

    private static void appendTime(StringBuilder sb, LocalTime localTime) {
        int hours = localTime.getHour();
        int minutes = localTime.getMinute();
        int seconds = localTime.getSecond();
        int nanos = localTime.getNano();
        appendTime(sb, hours, minutes, seconds, nanos);
    }

    private static void appendTimeZone(StringBuilder sb, ZoneOffset offset) {
        int offsetSeconds = offset.getTotalSeconds();

        appendTimeZone(sb, offsetSeconds);
    }

    private static void appendTimeZone(StringBuilder sb, Calendar cal) {
        int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / 1000;

        appendTimeZone(sb, offset);
    }

    private static void appendTimeZone(StringBuilder sb, int offset) {
        int absoff = Math.abs(offset);
        int hours = absoff / 60 / 60;
        int mins = (absoff - hours * 60 * 60) / 60;
        int secs = absoff - hours * 60 * 60 - mins * 60;

        sb.append((offset >= 0) ? "+" : "-");

        sb.append(NUMBERS[hours]);

        if (mins == 0 && secs == 0) {
            return;
        }
        sb.append(':');

        sb.append(NUMBERS[mins]);

        if (secs != 0) {
            sb.append(':');
            sb.append(NUMBERS[secs]);
        }
    }

    private static class ParsedTimestamp {
        private enum Type {
            DATE, TIME, DATE_TIME
        }
        int era = GregorianCalendar.AD;
        int year = 1970;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int nanos = 0;


        // extra fields
        Calendar calendar;
        Type type;
    }

    private ParsedTimestamp parse(String str) throws ProtocolException {
        try {
            ParsedTimestamp result = new ParsedTimestamp();
            int start = skipWhitespace(str, 0);
            int end = firstNonDigit(str, start);

            if (charAt(str, end) == '-') { // read date
                setHasDate(result);

                result.year = Integer.parseInt(str.substring(start, end));

                start = end + 1;
                end = firstNonDigit(str, start);

                if (charAt(str, end) != '-')
                    throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Expected date to be dash-separated, got '" + charAt(str, end) + "'");

                result.month = Integer.parseInt(str.substring(start, end));

                start = end + 1;
                end = firstNonDigit(str, start);

                result.day = Integer.parseInt(str.substring(start, end));

                start = skipWhitespace(str, end);
            }

            if (Character.isDigit(charAt(str, start))) { // read time
                setHasTime(result);

                end = firstNonDigit(str, start);

                if (charAt(str, end) != ':')
                    throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Expected time to be colon-separated, got '" + charAt(str, end) + "'");

                result.hour = Integer.parseInt(str.substring(start, end));

                start = end + 1;
                end = firstNonDigit(str, start);

                if (charAt(str, end) != ':')
                    throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Expected time to be colon-separated, got '" + charAt(str, end) + "'");

                result.minute = Integer.parseInt(str.substring(start, end));

                start = end + 1;
                end = firstNonDigit(str, start);

                result.second = Integer.parseInt(str.substring(start, end));

                if (charAt(str, end) == '.') {
                    start = end + 1;
                    end = firstNonDigit(str, start);

                    result.nanos = Integer.parseInt(str.substring(start, end));

                    for (int numLen = end - start; numLen < 9; ++numLen) {
                        result.nanos *= 10;
                    }
                }

                start = skipWhitespace(str, end);
            }

            char c = charAt(str, start);
            if (c == '+' || c == '-') { // read timezone
                int sign = (c == '-') ? -1 : 1;
                int hour;
                int minute = 0;
                int second = 0;

                end = firstNonDigit(str, ++start);
                hour = Integer.parseInt(str.substring(start, end));
                start = end;

                if (charAt(str, start) == ':') {
                    end = firstNonDigit(str, ++start);
                    minute = Integer.parseInt(str.substring(start, end));
                    start = end;
                }

                if (charAt(str, start) == ':') {
                    end = firstNonDigit(str, ++start);
                    second = Integer.parseInt(str.substring(start, end));
                    start = end;
                }

                result.calendar = asCalendar(sign, hour, minute, second);

                start = skipWhitespace(str, start);
            }

            if (result.type == null)
                throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Timestamp has neither date nor time");

            if (result.type != ParsedTimestamp.Type.TIME && start != str.length()) { // read era
                String eraString = str.substring(start);
                if (eraString.startsWith("AD")) {
                    result.era = GregorianCalendar.AD;
                    start += 2;
                } else if (eraString.startsWith("BC")) {
                    result.era = GregorianCalendar.BC;
                    start += 2;
                }
            }

            if (start < str.length())
                throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Trailing junk on timestamp: '" + str.substring(start) + "'");

            return result;
        } catch (NumberFormatException e) {
            throw new NonBreakingException(ErrorCodes.BAD_DATETIME_FORMAT, "Failed to read object", e);
        }
    }

    public LocalTime parseLocalTime(String str) throws ProtocolException {
        if (str == null) {
            return null;
        }

        ParsedTimestamp ts = parse(str);

        if (ts.hour == 24 && ts.minute == 0 && ts.second == 0 && ts.nanos == 0)
            ts.hour = 0;

        return LocalTime.of(ts.hour, ts.minute, ts.second, ts.nanos);
    }

    public LocalDate parseLocalDate(String str) throws ProtocolException {
        if (str == null) {
            return null;
        }

        int slen = str.length();

        // convert postgres's infinity values to internal infinity magic value
        if (slen == 8 && str.equals("infinity")) {
            return LocalDate.MAX;
        }

        if (slen == 9 && str.equals("-infinity")) {
            return LocalDate.MIN;
        }

        ParsedTimestamp ts = parse(str);

        LocalDate result = LocalDate.of(ts.year, ts.month, ts.day);

        if (ts.era == GregorianCalendar.BC) {
            return result.with(ChronoField.ERA, IsoEra.BCE.getValue());
        } else {
            return result;
        }
    }

    public LocalDateTime parseLocalDateTime(String str) throws ProtocolException {
        if (str == null) {
            return null;
        }

        int slen = str.length();

        // convert postgres's infinity values to internal infinity magic value
        if (slen == 8 && str.equals("infinity")) {
            return LocalDateTime.MAX;
        }

        if (slen == 9 && str.equals("-infinity")) {
            return LocalDateTime.MIN;
        }

        ParsedTimestamp ts = parse(str);

        // intentionally ignore time zone
        // 2004-10-19 10:23:54+03:00 is 2004-10-19 10:23:54 locally
        LocalDateTime result = LocalDateTime.of(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.nanos);
        if (ts.era == GregorianCalendar.BC) {
            return result.with(ChronoField.ERA, IsoEra.BCE.getValue());
        } else {
            return result;
        }
    }

    public OffsetDateTime parseOffsetDateTime(String str) throws ProtocolException {
        if (str == null) {
            return null;
        }

        int slen = str.length();

        // convert postgres's infinity values to internal infinity magic value
        if (slen == 8 && str.equals("infinity")) {
            return OffsetDateTime.MAX;
        }

        if (slen == 9 && str.equals("-infinity")) {
            return OffsetDateTime.MIN;
        }

        ParsedTimestamp ts = parse(str);

        Calendar calendar = ts.calendar;
        int offsetSeconds;
        if (calendar == null) {
            offsetSeconds = 0;
        } else {
            offsetSeconds = calendar.get(Calendar.ZONE_OFFSET) / 1000;
        }
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(offsetSeconds);
        OffsetDateTime result = OffsetDateTime.of(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.nanos, zoneOffset);
        if (ts.era == GregorianCalendar.BC) {
            return result.with(ChronoField.ERA, IsoEra.BCE.getValue());
        } else {
            return result;
        }
    }

    public Instant parseInstant(String str) throws ProtocolException {
        return parseOffsetDateTime(str).toInstant();
    }

    public OffsetTime parseOffsetTime(String str) throws ProtocolException {
        if (str == null) {
            return null;
        }

        ParsedTimestamp ts = parse(str);

        Calendar calendar = ts.calendar;
        int offsetSeconds;
        if (calendar == null) {
            offsetSeconds = 0;
        } else {
            offsetSeconds = calendar.get(Calendar.ZONE_OFFSET) / 1000;
        }
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(offsetSeconds);

        if (ts.hour == 24 && ts.minute == 0 && ts.second == 0 && ts.nanos == 0)
            ts.hour = 0;

        return OffsetTime.of(ts.hour, ts.minute, ts.second, ts.nanos, zoneOffset);
    }

    public long toPgMicros(java.util.Date date) {
        return toPgMicros(date.getTime()); // number of microseconds from PG epoch
    }

    public long toPgMicros(Calendar calendar) {
        return toPgMicros(calendar.getTimeInMillis() + calendar.getTimeZone().getRawOffset()); // number of microseconds from PG epoch
    }

    public long toPgMicros(Instant instant) {
        return toPgMicros(instant.getEpochSecond(), instant.getNano()); // number of microseconds from PG epoch
    }

    public int toPgMicros(LocalDate date) {
        long javaEpochSecs = date.toEpochDay() * 86400;
        return (int) toPgEpoch(javaEpochSecs) / 86400; // number of days from PG epoch
    }

    public long toPgMicros(LocalTime localTime) {
        return localTime.toNanoOfDay() / 1000; // number of microseconds from start of the day
    }

    public long toPgMicros(LocalDateTime localDateTime) {
        return toPgMicros(localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano()); // number of microseconds from PG epoch
    }

    public int toPgMicros(Date date) {
        return toPgMicros(date.toLocalDate()); // number of days from PG epoch
    }

    public long toPgMicros(Time time) {
        return toPgMicros(time.toLocalTime()); // number of microseconds from start of the day
    }

    public long toPgMicros(Timestamp timestamp) {
        return toPgMicros(timestamp.toLocalDateTime());
    }

    public int toPgOffset(ZoneOffset offset) {
        return offset.getTotalSeconds();
    }

    public int getPgOffset(Calendar calendar) {
        return calendar.getTimeZone().getRawOffset() / 1000;
    }

    public LocalDate toLocalDate(int pgDays) {
        long pgEpochSecs = pgDays * 86400L;
        return LocalDate.ofEpochDay(toJavaEpoch(pgEpochSecs) / 86400);
    }

    public LocalTime toLocalTime(long pgMicros) {
        return LocalTime.ofNanoOfDay(pgMicros * 1000);
    }

    public LocalDateTime toLocalDateTime(long pgMicros) {
        return LocalDateTime.ofEpochSecond(toJavaSecs(pgMicros), getNanoPart(pgMicros), ZoneOffset.UTC);
    }

    public ZoneOffset toJavaOffset(int pgOffset) {
        return ZoneOffset.ofTotalSeconds(pgOffset);
    }

    private static int skipWhitespace(String str, int offset) {
        for (int i = offset; i < str.length(); i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return i;
            }
        }
        return str.length();
    }

    private static int firstNonDigit(String str, int offset) {
        for (int i = offset; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return i;
            }
        }
        return str.length();
    }

    private static char charAt(String str, int idx) {
        return idx >= str.length() ? '\0' : str.charAt(idx);
    }

    private static void setHasDate(ParsedTimestamp timestamp) {
        if (timestamp.type == null) {
            timestamp.type = ParsedTimestamp.Type.DATE;

            return;
        }

        switch (timestamp.type) {
            case DATE:
            case DATE_TIME:
                return;

            case TIME:
                timestamp.type = ParsedTimestamp.Type.DATE_TIME;
        }
    }

    private static void setHasTime(ParsedTimestamp timestamp) {
        if (timestamp.type == null) {
            timestamp.type = ParsedTimestamp.Type.TIME;

            return;
        }

        switch (timestamp.type) {
            case TIME:
            case DATE_TIME:
                return;

            case DATE:
                timestamp.type = ParsedTimestamp.Type.DATE_TIME;
        }
    }

    private Calendar asCalendar(int sign, int hr, int min, int sec) {
        int rawOffset = sign * (((hr * 60 + min) * 60 + sec) * 1000);

        if (tmpCalendar == null || tmpOffset != rawOffset) {
            StringBuilder zoneID = new StringBuilder("GMT");
            zoneID.append(sign < 0 ? '-' : '+');
            if (hr < 10) {
                zoneID.append('0');
            }
            zoneID.append(hr);
            if (min < 10) {
                zoneID.append('0');
            }
            zoneID.append(min);
            if (sec < 10) {
                zoneID.append('0');
            }
            zoneID.append(sec);

            TimeZone syntheticTZ = new SimpleTimeZone(rawOffset, zoneID.toString());
            tmpCalendar = new GregorianCalendar(syntheticTZ);
            tmpOffset = rawOffset;
        }

        return tmpCalendar;
    }

    private static long toPgMicros(long javaMillis) {
        return toPgMicros(javaMillis / 1000, (int) ((javaMillis % 1000) * 1000000));
    }

    private static long toJavaSecs(long pgMicros) {
        return toJavaEpoch(pgMicros / 1000000);
    }

    private static long toPgMicros(long javaSecs, int nanos) {
        // Round to microseconds
        if (nanos >= MAX_NANOS_BEFORE_WRAP_ON_ROUND) {
            nanos = 0;
            javaSecs++;
        } else if (nanosExceed499(nanos)) {
            // PostgreSQL does not support nanosecond resolution yet, and appendTime will just ignore
            // 0..999 part of the nanoseconds, however we subtract nanos % 1000 to make the value
            // a little bit saner for debugging reasons
            nanos += 1000 - nanos % 1000;
        }

        return toPgEpoch(javaSecs) * 1000000 + nanos / 1000;
    }

    private static long toPgEpoch(long javaEpochSecs) {
        // java epoc to postgres epoc (January 1, 2000 or 2000-01-01)
        javaEpochSecs -= 946684800L;

        // Julian/Greagorian calendar cutoff point
        if (javaEpochSecs < -13165977600L) { // October 15, 1582 -> October 4, 1582
            javaEpochSecs -= 86400 * 10;
            if (javaEpochSecs < -15773356800L) { // 1500-03-01 -> 1500-02-28
                int years = (int) ((javaEpochSecs + 15773356800L) / -3155823050L);
                years++;
                years -= years / 4;
                javaEpochSecs += years * 86400L;
            }
        }

        return javaEpochSecs;
    }

    private static long toJavaEpoch(long pgEpochSecs) {
        // postgres epoc (January 1, 2000 or 2000-01-01) to java epoc
        pgEpochSecs += 946684800L;

        // Julian/Gregorian calendar cutoff point
        if (pgEpochSecs < -12219292800L) { // October 4, 1582 -> October 15, 1582
            pgEpochSecs += 86400 * 10;
            if (pgEpochSecs < -14825808000L) { // 1500-02-28 -> 1500-03-01
                int extraLeaps = (int) ((pgEpochSecs + 14825808000L) / 3155760000L);
                extraLeaps--;
                extraLeaps -= extraLeaps / 4;
                pgEpochSecs += extraLeaps * 86400L;
            }
        }
        return pgEpochSecs;
    }

    private static int getNanoPart(long pgMicros) {
        return (int) ((pgMicros % 1000000) * 1000);
    }
}
