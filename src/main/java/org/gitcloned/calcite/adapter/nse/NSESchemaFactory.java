package org.gitcloned.calcite.adapter.nse;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class NSESchemaFactory implements SchemaFactory {

    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {

        NSESchema schema = new NSESchema((String)map.get("group"));
        return schema;
    }
}
