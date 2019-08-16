
## Use JDBC driver to query Rest APIs

This is a simple project which explains the usage of [Apache Calcite](https://calcite.apache.org/) to query [NSE](https://www.nseindia.com/live_market/dynaContent/live_watch/equities_stock_watch.htm) data APIs using JDBC interface. The similar concepts could be used to query any rest API.

For detailed explanation, read [blog](https://medium.com/@ashishjain.ash/how-to-create-a-jdbc-driver-over-rest-apis-1571ab156e6a)

### How to run

To run the project, clone it in a folder and make sure to have below pre-requisites installed and setup:

 * JAVA 1.8 or above
 * Maven 3.6 or above

**Build the project**

```shell
mvn clean install -DskipTests
```

**Run [Sqlline](https://github.com/julianhyde/sqlline)**

A command line utility for issuing SQL to relational databases via JDBC.

```shell
./sqlline
```

*if using Windows OS, then use `sqlline.bat` file*

This would open a sqlline shell, which can now be used to connect to JDBC driver and issue SQL queries

*image here*

**Create a model.json file**

Define a model.json file, which should define the schema and the schema factory class, as:

```json
{
  "version": "1.0",
  "defaultSchema": "BroadMarket",
  "schemas": [
    {
      "name": "BroadMarket",
      "type": "custom",
      "factory": "org.gitcloned.calcite.adapter.nse.NSESchemaFactory",
      "operand": {
		"group": "Broad Market Indices"
      }
    }
  ]
}
```

**Connect to JDBC using model.json**

```shell
!connect jdbc:calcite:model=/path/to/model.json admin admin
```

This will connect to the JDBC schema defined in model.json file, to list the tables run `!tables` command, which will list all tables along with some system tables:

```sql
+-----------+-------------+---------------------------------+--------------+---+

| TABLE_CAT | TABLE_SCHEM |           TABLE_NAME            |  TABLE_TYPE  | R |

+-----------+-------------+---------------------------------+--------------+---+

|           | BroadMarket | NIFTY JUNIOR                    | TABLE        |   |

|           | BroadMarket | NIFTY MIDCAP 150                | TABLE        |   |

|           | BroadMarket | NIFTY MIDCAP 50                 | TABLE        |   |

|           | BroadMarket | NIFTY SMLCAP 250                | TABLE        |   |

|           | BroadMarket | NIFTY SMLCAP 50                 | TABLE        |   |

|           | BroadMarket | NIFTY50                         | TABLE        |   |

|           | Others      | SOVEREIGN GOLD BONDS            | TABLE        |   |

|           | Sectoral    | NIFTY AUTO                      | TABLE        |   |

|           | Sectoral    | NIFTY BANK                      | TABLE        |   |

|           | Sectoral    | NIFTY ENERGY                    | TABLE        |   |

|           | Sectoral    | NIFTY FINANCIAL SERVICES        | TABLE        |   |

|           | Sectoral    | NIFTY FMCG                      | TABLE        |   |

|           | Sectoral    | NIFTY IT                        | TABLE        |   |

|           | Sectoral    | NIFTY METAL                     | TABLE        |   |

|           | Sectoral    | NIFTY PHARMA                    | TABLE        |   |

|           | Sectoral    | NIFTY REALTY                    | TABLE        |   |

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | TABLE        |   |

|           | Thematic    | NIFTY COMMODITIES               | TABLE        |   |

|           | metadata    | COLUMNS                         | SYSTEM TABLE |   |

|           | metadata    | TABLES                          | SYSTEM TABLE |   |

+-----------+-------------+---------------------------------+--------------+---+
```

To list available columns, run `!columns` command:

```sql
+-----------+-------------+---------------------------------+------------------+

| TABLE_CAT | TABLE_SCHEM |           TABLE_NAME            |      COLUMN_NAME |

+-----------+-------------+---------------------------------+------------------+

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | trdVolM          |

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | wkhi             |

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | wklo             |

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | yPC              |

|           | Strategy    | NIFTY DIVIDEND OPPORTUNITIES 50 | time             |

|           | Thematic    | NIFTY COMMODITIES               | symbol           |

|           | Thematic    | NIFTY COMMODITIES               | open             |

|           | Thematic    | NIFTY COMMODITIES               | high             |

|           | Thematic    | NIFTY COMMODITIES               | low              |

|           | Thematic    | NIFTY COMMODITIES               | previousClose    |

|           | Thematic    | NIFTY COMMODITIES               | ltp              |

|           | Thematic    | NIFTY COMMODITIES               | per              |

|           | Thematic    | NIFTY COMMODITIES               | trdVolM          |

|           | Thematic    | NIFTY COMMODITIES               | wkhi             |

|           | Thematic    | NIFTY COMMODITIES               | wklo             |

|           | Thematic    | NIFTY COMMODITIES               | yPC              |

|           | Thematic    | NIFTY COMMODITIES               | time             |
+-----------+-------------+---------------------------------+------------------+
```

Next, try SQL queries over data

### Other Details

 * Log File: application.log
