import org.junit.Test;
import sqlline.BuiltInProperty;
import sqlline.DispatchCallback;
import sqlline.SqlLine;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class RestSQL {

  public static void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // Print column names
    for (int i = 1; i <= columnCount; i++) {
      System.out.print(metaData.getColumnName(i) + "\t");
    }
    System.out.println();

    // Print each row of data
    while (rs.next()) {
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(rs.getString(i) + "\t");
      }
      System.out.println();
    }
  }

  @Test
  public void jdbc() throws Exception {
    SqlLine sqlLine = new SqlLine();
    sqlLine.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    sqlLine.getOpts().set(BuiltInProperty.MAX_WIDTH, 120);

    String url = "jdbc:calcite:model=src/main/resources/model.json";
    String user = "admin";
    String password = "admin";

    Connection connection = DriverManager.getConnection(url, user, password);

    DatabaseMetaData metadata = connection.getMetaData();
    System.out.println("Database product name: " + metadata.getDatabaseProductName());
    System.out.println("Default transaction isolation: " + metadata.getDefaultTransactionIsolation());

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("select \"open\" from NIFTY_50");
    printResultSet(rs);

    ResultSet rs2 = stmt.executeQuery("SELECT * FROM \"Sectoral\".\"NIFTY_ENERGY\" INNER JOIN \"NIFTY_50\" ON \"Sectoral\".\"NIFTY_ENERGY\".\"symbol\" = \"NIFTY_50\".\"symbol\"");
    printResultSet(rs2);
  }

  @Test
  public void sqlline() {
    SqlLine sqlLine = new SqlLine();
    sqlLine.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    sqlLine.getOpts().set(BuiltInProperty.MAX_WIDTH, 120);

    DispatchCallback callback = new DispatchCallback();
    sqlLine.runCommands(Arrays.asList("!connect jdbc:calcite:model=src/main/resources/model.json admin admin",
                                      "select \"open\" from NIFTY_50;",
                                      "SELECT * FROM \"Sectoral\".\"NIFTY_ENERGY\" INNER JOIN \"NIFTY_50\" ON \"Sectoral\".\"NIFTY_ENERGY\".\"symbol\" = \"NIFTY_50\".\"symbol\""
    ), callback);
  }
}
