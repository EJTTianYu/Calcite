/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsCsvMulEnumerator<E> implements Enumerator<E> {

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  private final AtomicBoolean cancelFlag;
  private final String[] filterValues;
  private final RowConverter<E> rowConverter;
  private BufferedReader reader;
  private E current;

  HdfsCsvMulEnumerator(String fileName, AtomicBoolean cancelFlag, boolean stream,
      String[] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    try {
      FileSystem fs = HdfsCsvMulSchema.getFileSystem();
      InputStream in = null;
      FSDataInputStream hdfsInStream = fs.open(new Path(fileName));
      InputStreamReader isr = new InputStreamReader(hdfsInStream, "utf-8");
      this.reader = new BufferedReader(isr);
      this.reader.readLine();//skip header row
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
   * Deduces the names and types of a table's columns by reading the first line of a CSV file.
   */
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, String fileName,
      List<HdfsCsvMulFieldType> fieldTypes) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    FileSystem fileSystem = HdfsCsvMulSchema.getFileSystem();
    try {
      FSDataInputStream hdfsInStream = fileSystem.open(new Path(fileName));
      InputStreamReader isr = new InputStreamReader(hdfsInStream, "utf-8");
      BufferedReader br = new BufferedReader(isr);
      String[] strings = br.readLine().split(",");
      if (strings == null) {
        strings = new String[]{"EmptyFileHasNoColumns:boolean"};
      }
      for (String string : strings) {
        final String name;
        final HdfsCsvMulFieldType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          fieldType = HdfsCsvMulFieldType.of(typeString);
          if (fieldType == null) {
            System.out.println("WARNING: Found unknown type: "
                + typeString + " in file: " + fileName
                + " for column: " + name
                + ". Will assume the type of column is string");
          }
        } else {
          name = string;
          fieldType = null;
        }
        final RelDataType type;
        if (fieldType == null) {
          type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else {
          type = fieldType.toType(typeFactory);
        }
        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /**
   * Row converter.
   *
   * @param <E> element type
   */
  abstract static class RowConverter<E> {

    abstract E convertRow(String[] rows);

    protected Object convert(HdfsCsvMulFieldType fieldType, String string) {
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
          if (string.length() == 0) {
            return null;
          }
          return Long.parseLong(string);
        case FLOAT:
          if (string.length() == 0) {
            return null;
          }
          return Float.parseFloat(string);
        case DOUBLE:
          if (string.length() == 0) {
            return null;
          }
          return Double.parseDouble(string);
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

    private final HdfsCsvMulFieldType[] fieldTypes;
    private final int[] fields;
    // whether the row to convert is from a stream
    private final boolean stream;

    ArrayRowConverter(List<HdfsCsvMulFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new HdfsCsvMulFieldType[0]);
      this.fields = fields;
      this.stream = false;
    }

    ArrayRowConverter(List<HdfsCsvMulFieldType> fieldTypes, int[] fields, boolean stream) {
      this.fieldTypes = fieldTypes.toArray(new HdfsCsvMulFieldType[0]);
      this.fields = fields;
      this.stream = stream;
    }

    public Object[] convertRow(String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public Object[] convertNormalRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
      final Object[] objects = new Object[fields.length + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i + 1] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  @Override
  public E current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    try {
      outer:
      for (; ; ) {
        if (cancelFlag.get()) {
          return false;
        }
        String line = reader.readLine();
        String[] strings = null;
        if (line != null) {
          strings = line.split(",");
        }
        if (strings == null) {
          current = null;
          reader.close();
          return false;
        }
        if (filterValues != null) {
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues[i];
            if (filterValue != null) {
              if (!filterValue.equals(strings[i])) {
                continue outer;
              }
            }
          }
        }
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    }
  }
}
