import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

enum HdfsFsFieldType {
  STRING(String.class, "string");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, HdfsFsFieldType> MAP = new HashMap<>();

  static {
    for (HdfsFsFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  HdfsFsFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  HdfsFsFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static HdfsFsFieldType of(String typeString) {
    return MAP.get(typeString);
  }

}