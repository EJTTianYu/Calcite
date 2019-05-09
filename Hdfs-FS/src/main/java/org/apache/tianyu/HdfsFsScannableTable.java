import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;

public class HdfsFsScannableTable extends HdfsFsTable implements ScannableTable {

  public HdfsFsScannableTable(String directory, int folderDepth, List<List<String>> tableList) {
    super(directory, folderDepth, tableList);
  }

  public String toString() {
    return "org.apache.tianyu.HdfsFsScannableTable";
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = HdfsFsEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new HdfsFsEnumerator<>(tableList, cancelFlag, false, null,
            new HdfsFsEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }

}
