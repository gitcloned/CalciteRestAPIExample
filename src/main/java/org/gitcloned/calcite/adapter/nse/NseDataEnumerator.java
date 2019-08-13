package org.gitcloned.calcite.adapter.nse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.time.FastDateFormat;
import org.gitcloned.nse.NseFieldType;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class NseDataEnumerator implements Enumerator<Object[]> {

    private final JsonArray rows;
    private final String[] filterValues;
    private final ArrayRowConverter rowConverter;

    private int index = -1;
    private JsonObject current;

    private boolean isClosed = false;

    public NseDataEnumerator(JsonArray results, List<NseFieldType> fieldTypes, List<String> fieldNames) {
        this(results, fieldTypes, fieldNames, identityList(fieldTypes.size()));
    }

    public NseDataEnumerator(JsonArray results, List<NseFieldType> fieldTypes, List<String> fieldNames, int[] fields) {

        this(results, null, converter(fieldTypes, fieldNames, fields));
    }

    public NseDataEnumerator(JsonArray results, String[] filterValues, ArrayRowConverter rowConverter) {

        rows = results;

        this.filterValues = filterValues;
        this.rowConverter = rowConverter;
    }

    private static ArrayRowConverter converter(List<NseFieldType> fieldTypes, List<String> fieldNames,
                                               int[] fields) {
        return new ArrayRowConverter(fieldTypes, fieldNames, fields);
    }

    private JsonObject getNextRow() {

        return rows.get(++index).getAsJsonObject();
    }

    @Override
    public Object[] current() {

        try {
            return rowConverter.convertNormalRow(current);
        } catch (ParseException e) {
            throw new RuntimeException("Failed at parsing a row: " + e.getMessage());
        }
    }

    @Override
    public boolean moveNext() {

        if ((index + 1) == rows.size()) return false;

        current = getNextRow();

        return true;
    }

    @Override
    public void reset() {
        current = null;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }


    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {

        private static final FastDateFormat TIME_FORMAT_DATE;
        private static final FastDateFormat TIME_FORMAT_TIME;
        private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

        static {
            final TimeZone gmt = TimeZone.getTimeZone("GMT");
            final TimeZone ist = TimeZone.getTimeZone("IST");
            TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
            TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
            TIME_FORMAT_TIMESTAMP =
                    // FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
                    FastDateFormat.getInstance("MMM dd, yyyy HH:mm:ss", ist);
        }

        abstract E convertRow(JsonObject row);

        protected Object convert(NseFieldType fieldType, String string) throws ParseException {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
                case BOOLEAN:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case BYTE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Byte.parseByte(string);
                case SHORT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Short.parseShort(string);
                case INT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
                case LONG:
                    /*
                    if (string.length() == 0) {
                        return null;
                    }
                    return Long.parseLong(string);
                    */
                case FLOAT:
                    // return Float.parseFloat(string);
                case DOUBLE:
                    if (string.length() == 0) {
                        return null;
                    }
                    string = NumberFormat.getNumberInstance(Locale.getDefault()).parse(string).toString();
                    if (fieldType == NseFieldType.LONG) {
                        return Long.parseLong(string);
                    } else if (fieldType == NseFieldType.DOUBLE) {
                        return Double.parseDouble(string);
                    } else if (fieldType == NseFieldType.FLOAT) {
                        return Float.parseFloat(string);
                    }
                // return Double.parseDouble(string);
                case DATE:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return (int) date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter<Object[]> {
        private final NseFieldType[] fieldTypes;
        private final String[] fieldNames;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;

        ArrayRowConverter(List<NseFieldType> fieldTypes, List<String> fieldNames, int[] fields) {
            this.fieldTypes = fieldTypes.toArray(new NseFieldType[0]);
            this.fieldNames = fieldNames.toArray(new String[0]);
            this.fields = fields;
            this.stream = false;
        }

        public Object[] convertRow(JsonObject row) {
            try {
                return convertNormalRow(row);
            } catch (ParseException e) {
                throw new RuntimeException("Failed at parsing a row: " + e.getMessage());
            }
        }

        public Object[] convertNormalRow(JsonObject row) throws ParseException {
            final Object[] objects = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                JsonElement value = row.get(fieldNames[i]);

                if (value == null || value.isJsonNull() || value.isJsonObject()) {
                    objects[i] = null;
                } else if (value.isJsonObject()) {
                    objects[i] = convert(fieldTypes[field], value.getAsJsonObject().toString());
                } else if (value.isJsonArray()) {
                    objects[i] = convert(fieldTypes[field], value.getAsJsonArray().toString());
                } else {
                    objects[i] = convert(fieldTypes[field], value.getAsString());
                }
            }
            return objects;
        }
    }

}
