import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsFsSchema extends AbstractSchema {

  private final String directory;

  public String getDirectory() {
    return directory;
  }

  public String getUserName() {
    return userName;
  }

  private final String userName;
  private final String password;
  private final int folderDepth;
  private Map<String, Table> tableMap;
  private final HdfsFsTable.Flavor flavor = HdfsFsTable.Flavor.SCANNABLE;
  private FileSystem fileSystem;

  public HdfsFsSchema(String host, int port, String directory, String userName,
      String password, int folderDepth) {
    super();
    this.directory = String.format("hdfs://%s:%s%s", host, port, directory);
    this.userName = userName;
    this.password = password;
    this.folderDepth = folderDepth;
    Configuration configuration = new Configuration();
    try {
      fileSystem = FileSystem
          .get(URI.create(this.directory), configuration, this.userName);

    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTabelMap();
    }
    return tableMap;
  }

  public Map<String, Table> createTabelMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    final Table table = createTable(this.directory, this.folderDepth);
    builder.put("rootPath", table);
    return builder.build();
  }

  private Table createTable(String directory, int folderDepth) {
    List<List<String>> tableList = generateTableList(fileSystem);
    switch (flavor) {
      case SCANNABLE:
        return new HdfsFsScannableTable(directory, folderDepth, tableList);
      default:
        throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }


  private List<List<String>> generateTableList(FileSystem fileSystem) {
    List<List<String>> tableList = new ArrayList<>();
    try {
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(directory), true);
      while (files.hasNext()) {
        LocatedFileStatus file = files.next();
        if (!file.isDirectory()) {
          String path = file.getPath().toString().replace(directory, "");
          if (path.split("/").length <= folderDepth + 1) {
            List<String> row = new ArrayList<>();
            for (int i = 0; i < folderDepth; i++) {
              if (i < path.split("/").length - 1) {
                row.add(path.split("/")[i]);
              } else {
                row.add("");
              }
            }
            row.add(path.split("/")[path.split("/").length-1]);
            tableList.add(row);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tableList;
  }
}
