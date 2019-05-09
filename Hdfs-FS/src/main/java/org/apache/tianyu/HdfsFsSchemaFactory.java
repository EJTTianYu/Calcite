import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

public class HdfsFsSchemaFactory implements SchemaFactory {

  /**
   * Public singleton, per factory contract.
   */
  public static final HdfsFsSchemaFactory Instance = new HdfsFsSchemaFactory();

  private HdfsFsSchemaFactory() {

  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    final String host = (String) operand.get("host");
    final int port = (Integer) operand.get("port");
    final String directory = (String) operand.get("directory");
    final String userName = (String) operand.get("userName");
    final String passWord = (String) operand.get("password");
    final int folderDepth = (Integer) operand.get("folderDepth");
    return new HdfsFsSchema(host, port, directory, userName, passWord, folderDepth);
  }
}
