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
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class HdfsCsvMulTable extends AbstractTable {

  public final String fileName;
  public List<HdfsCsvMulFieldType> fieldTypes;

  HdfsCsvMulTable(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return HdfsCsvMulEnumerator.deduceRowType((JavaTypeFactory) typeFactory, fileName,
          fieldTypes);
    } else {
      return HdfsCsvMulEnumerator.deduceRowType((JavaTypeFactory) typeFactory, fileName, null);
    }
  }

  /**
   * Various degrees of table "intelligence".
   */
  public enum Flavor {
    SCANNABLE
  }
}
