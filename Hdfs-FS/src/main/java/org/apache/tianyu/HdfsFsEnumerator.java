import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsFsEnumerator<E> implements Enumerator<E> {

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
  private final List<List<String>> tableList;
  private BufferedReader reader;
  private int rowPointer = 0;
  private E current;

  HdfsFsEnumerator(List<List<String>> tableList, AtomicBoolean cancelFlag, boolean stream,
      String[] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    this.tableList = tableList;
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
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, int folderDepth,
      List<HdfsFsFieldType> fieldTypes) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    for (int i = 1; i < folderDepth + 1; i++) {
      names.add("Level" + i);
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    names.add("fileName");
    types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    if (fieldTypes != null) {
      for (int i=0;i<folderDepth+1;i++)fieldTypes.add(HdfsFsFieldType.STRING);
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

    protected Object convert(HdfsFsFieldType fieldType, String string) {
      return string;
    }
  }

  /**
   * Array row converter.
   */
  static class ArrayRowConverter extends RowConverter<Object[]> {

    private final HdfsFsFieldType[] fieldTypes;
    private final int[] fields;
    // whether the row to convert is from a stream
    private final boolean stream;

    ArrayRowConverter(List<HdfsFsFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new HdfsFsFieldType[0]);
      this.fields = fields;
      this.stream = false;
    }

    ArrayRowConverter(List<HdfsFsFieldType> fieldTypes, int[] fields, boolean stream) {
      this.fieldTypes = fieldTypes.toArray(new HdfsFsFieldType[0]);
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
//    return current;
    return current;
  }

  @Override
  public boolean moveNext() {
    if (rowPointer < tableList.size()) {
      try {
        outer:
        for (; ; ) {
          if (cancelFlag.get()) {
            return false;
          }
          List<String> line = tableList.get(rowPointer);
          String[] tmps = new String[tableList.get(rowPointer).size()];
          String[] strings = line.toArray(tmps);

          if (strings == null) {
            current = null;
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
          rowPointer += 1;
          return true;
        }
      }
      finally{}
    }
    else return false;
  }


  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }
}
