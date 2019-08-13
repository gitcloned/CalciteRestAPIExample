package org.gitcloned.calcite.adapter.nse;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.gitcloned.nse.NseData;
import org.gitcloned.nse.http.NseHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public NSESchema(String group) {
        this.group = group;

        NseHTTP nseHttp = new NseHTTP("https://www.nseindia.com/live_market/dynaContent/live_watch/stock_watch/");

        this.nse = new NseData(nseHttp);
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
         * List all tables under different categories in NSE
         */
        if (this.group.equals("Broad Market Indices")) {

            NSEScannableTable table = (NSEScannableTable) createTable("nifty", "Nifty50");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("juniorNifty", "Nifty Junior");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("niftyMidcap50", "Nifty Midcap 50");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("niftyMidcap150Online", "Nifty Midcap 150");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("niftySmallcap50Online", "Nifty Smlcap 50");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("niftySmallcap250Online", "Nifty Smlcap 250");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
        } else if (this.group.equals("Sectoral Indices")) {

            NSEScannableTable table = (NSEScannableTable) createTable("cnxAuto", "Nifty Auto");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("bankNifty", "Nifty Bank");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxEnergy", "Nifty Energy");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxFinance", "Nifty Financial Services");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxFMCG", "Nifty FMCG");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxit", "Nifty IT");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxMetal", "Nifty Metal");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxPharma", "Nifty Pharma");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            table = (NSEScannableTable) createTable("cnxRealty", "Nifty Realty");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
        } else if (this.group.equals("Thematic Indices")) {

            NSEScannableTable table = (NSEScannableTable) createTable("cnxCommodities", "Nifty Commodities");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
        } else if (this.group.equals("Strategy Indices")) {

            NSEScannableTable table = (NSEScannableTable) createTable("cnxDividendOppt", "Nifty Dividend Opportunities 50");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
        } else {

            NSEScannableTable table = (NSEScannableTable) createTable("sovGold", "Sovereign Gold Bonds");
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);
        }

        return builder.build();
    }

    private Table createTable (String table, String name) {

        return new NSEScannableTable(this, group, table, name);
    }
}
