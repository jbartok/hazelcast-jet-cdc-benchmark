package helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class Runner {

    public void run() throws SQLException {
        String url = "jdbc:mysql://192.168.0.32:3306/inventory?rewriteBatchedStatements=true";
        String user = "mysqluser";
        String password = "mysqlpw";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            run(conn);
        }
    }

    public abstract void run(Connection conn) throws SQLException;

}
