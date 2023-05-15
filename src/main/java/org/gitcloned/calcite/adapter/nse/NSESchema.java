package org.gitcloned.calcite.adapter.nse;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.gitcloned.nse.NseData;
import org.gitcloned.nse.http.NseHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.Locale;
import java.util.Map;

/**
 * NSESchema class
 */
public class NSESchema extends AbstractSchema {

    Logger logger = LoggerFactory.getLogger(NSESchema.class);

    private final String group;

    private Map<String, Table> tableMap;

    private final NseData nse;
    private final String MASTER_SCHEMA_URL = "https://www.nseindia.com/api/equity-master";

    public NSESchema(String group) {
        this.group = group;

        NseHTTP nseHttp = new NseHTTP("https://www.nseindia.com", group);
        this.nse = new NseData("https://www.nseindia.com/api/equity-stockIndices?index=", nseHttp);
    }

    public NseData getNseSession() {
        return nse;
    }

    /**
     * Gets the table map under this schema
     * @return Map of TableName and Table object
     */
    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    /**
     * Creates the table map for the specified NSE group
     * @return Map of tableName and NSEScannableTable
     */
    private Map<String, Table> createTableMap() {

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        logger.debug(String.format("getting tables for group: '%s'", this.group));

        /**
         * List all tables under different categories in NSE from https://www.nseindia.com/api/equity-master
         */
        try {
            String masterJson = this.nse.getRequestBuilder().sendGetRequest(MASTER_SCHEMA_URL).getContentAsString();

            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse(masterJson);

            if (tree.isJsonObject()) {

               for(String groupName : ((JsonObject)tree).keySet()) {
                   if(this.group.equals(groupName)) {
                       logger.info(String.format("Loading group '%s'", groupName));
                       JsonElement groupData = ((JsonObject)tree).get(groupName);
                       if (groupData.isJsonArray()) {
                           JsonArray group = groupData.getAsJsonArray();
                           for (JsonElement tableData : group) {
                               if (tableData.isJsonPrimitive()) {
                                   String tableName = tableData.getAsString();
                                   logger.info(String.format("Loading group '%s' table '%s'", groupName, tableName));

                                   NSEScannableTable table = (NSEScannableTable) createTable(tableName, tableName.replace(" ", "_"));
                                   builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
                               }
                           }
                       }
                   }
               }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    private Table createTable (String table, String name) {

        return new NSEScannableTable(this, group, table, name);
    }
}
