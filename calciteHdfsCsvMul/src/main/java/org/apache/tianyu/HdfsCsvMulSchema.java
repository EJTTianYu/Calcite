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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsCsvMulSchema extends AbstractSchema {

  private final String directory;
  private final String userName;
  private final String password;
  private Map<String, Table> tableMap;

  public static FileSystem getFileSystem() {
    return fileSystem;
  }

  private static FileSystem fileSystem;
  private final HdfsCsvMulTable.Flavor flavor = HdfsCsvMulTable.Flavor.SCANNABLE;


  public HdfsCsvMulSchema(String host, int port, String directory, String userName,
      String password) {
    super();
    this.directory = String.format("hdfs://%s:%s%s", host, port, directory);
    this.userName = userName;
    this.password = password;
    Configuration configuration = new Configuration();
    try {
      this.fileSystem = FileSystem.get(URI.create(this.directory), configuration, this.userName);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public String getDirectory() {
    return directory;
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
    try {
      RemoteIterator<LocatedFileStatus> csvFiles = fileSystem.listFiles(new Path(directory), true);
      while (csvFiles.hasNext()) {
        LocatedFileStatus file = csvFiles.next();
        if (!file.isDirectory() && file.getPath().toString().endsWith(".csv")) {
          String path = file.getPath().toString().replace(directory, "");
          final Table table = createTable(file.getPath().toString());
          builder.put(path, table);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return builder.build();
  }

  private Table createTable(String fileName) {
    switch (flavor) {
      case SCANNABLE:
        return new HdfsCsvMulScannableTable(fileName);
      default:
        throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }


}
