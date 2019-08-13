package org.gitcloned.calcite.adapter.nse;

import com.google.gson.JsonArray;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.gitcloned.nse.NseFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NSEScannableTable extends AbstractTable
        implements ScannableTable {

    private final NSESchema nseSchema;
    private final String group;
    private final String table;
    private final String name;

    protected List<String> fieldNames;
    protected List<NseFieldType> fieldTypes;

    private List<RelDataType> fields = new ArrayList<>();

    public NSEScannableTable(NSESchema nseSchema, String group, String table, String name) {
        this.nseSchema = nseSchema;
        this.group = group;
        this.table = table;
        this.name = name;
    }

    public String getTableName() {
        return this.name;
    }

    public String getTable() {
        return this.table;
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {

        try {
            final JsonArray results = this.nseSchema.getNseSession().scanTable(this.group, this.table);

            return new AbstractEnumerable<Object[]>() {
                public Enumerator<Object[]> enumerator() {

                    Enumerator<Object[]> enumerator = new NseDataEnumerator(results, fieldTypes, fieldNames);
                    return enumerator;
                }
            };

        } catch (IOException e) {
            throw new RuntimeException("Got IO exception: " + e.getMessage());
        }
    }

    /**
     * Get fields and their types in a row
     * @param typeFactory
     * @return
     */
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        if (fieldTypes == null || fieldNames == null) {

            fieldTypes = new ArrayList<>();
            fieldNames = new ArrayList<>();

            fieldTypes.add(NseFieldType.STRING);
            fields.add(NseFieldType.STRING.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("symbol");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("open");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("high");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("low");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("previousClose");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("ltp");

            fieldTypes.add(NseFieldType.FLOAT);
            fields.add(NseFieldType.FLOAT.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("per");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("trdVolM");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("wkhi");

            fieldTypes.add(NseFieldType.DOUBLE);
            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("wklo");

            fieldTypes.add(NseFieldType.FLOAT);
            fields.add(NseFieldType.FLOAT.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("yPC");

            fieldTypes.add(NseFieldType.TIMESTAMP);
            fields.add(NseFieldType.TIMESTAMP.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("time");
        }

        return typeFactory.createStructType(Pair.zip(fieldNames, fields));
    }
}
