package sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import in.org.iudx.adaptor.datatypes.Message;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomSchema extends AbstractSchema {
    public List<Message> state;

//    private static final ObjectMapper mapper = new ObjectMapper();
//
//    private static final Map<Object, ObjectNode> employees = new HashMap<>();
//
//    static {
//        employees.put(1L, mapper.createObjectNode().put("name", "john").put("age", 30));
//        employees.put(2L, mapper.createObjectNode().put("name", "jane").put("age", 25));
//        employees.put(3L, mapper.createObjectNode().put("name", "cole").put("age", 50));
//    }

//    @Override
//    protected Map<String, Table> getTableMap() {
//        return Collections.singletonMap("employees", new CustomTable(employees));
//    }
}