import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class HdfsFsTable extends AbstractTable {

  public final int folderDepth;
  public final String directory;
  public List<HdfsFsFieldType> fieldTypes;
  public List<List<String>> tableList;

  HdfsFsTable(String directory, int folderDepth,List<List<String>> tableList) {
    this.directory = directory;
    this.folderDepth = folderDepth;
    this.tableList=tableList;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return HdfsFsEnumerator.deduceRowType((JavaTypeFactory) typeFactory, folderDepth,
          fieldTypes);
    } else {
      return HdfsFsEnumerator.deduceRowType((JavaTypeFactory) typeFactory, folderDepth, null);
    }
  }

  /**
   * Various degrees of table "intelligence".
   */
  public enum Flavor {
    SCANNABLE
  }
}
