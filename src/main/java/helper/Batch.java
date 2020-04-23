package helper;

import java.sql.SQLException;

public interface Batch {

    int run() throws SQLException;

}
