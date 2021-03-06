package kr.co.penta.datalake.types;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import kr.co.penta.datalake.common.Aes;

public class Application {

    public static void main(String[] args) {

        String className = null;
        String url = "jdbc:sqlite::memory:";
        String username = null;
        String password = null;
        int fetchsize = 1000;

        Connection connection;
        DatabaseMetaData version;
        Statement statement;
        ResultSet result;
        ResultSetMetaData meta;
        int index;
        int count;

        if (System.getProperty("jdbc.classname") != null)
            className = System.getProperty("jdbc.classname");
        if (System.getProperty("jdbc.url") != null)
            url = System.getProperty("jdbc.url");
        if (System.getProperty("jdbc.username") != null)
            username = System.getProperty("jdbc.username");
        if (System.getProperty("jdbc.password") != null)
            password = Aes.password(System.getProperty("jdbc.password"));
        if (System.getProperty("jdbc.fetchsize") != null)
            fetchsize = Integer.parseInt(System.getProperty("jdbc.fetchsize"));

        try {
            if (className != null) {
                Class.forName(className);
            }
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        try {

            int connect_retry = 0;

            do {
                try {
                    connection = DriverManager.getConnection(url, username, password);
                } catch (Exception e) {
                    System.err.println(e);
                    connect_retry++;
                    if (connect_retry > 5) {
                        System.exit(1);
                    }
                    Thread.sleep(1000);
                    connection = null;
                }
            } while (connection == null);

            if (args.length > 0) {

                statement = connection.createStatement();
                statement.setFetchSize(fetchsize);

                result = null;
                if (statement.execute(args[0]) == true) {
                    result = statement.getResultSet();
                }

                if (result != null) {
                    meta = result.getMetaData();
                    count = meta.getColumnCount();
                    for (index = 1; index <= count; index++) {
                        System.out.print(meta.getColumnName(index));
                        System.out.print(" ");
                        System.out.print(meta.getColumnType(index));
                        System.out.print(" ");
                        System.out.print(meta.getColumnTypeName(index));
                        System.out.print("(");
                        System.out.print(meta.getPrecision(index));
                        System.out.print(",");
                        System.out.print(meta.getScale(index));
                        System.out.print(")\n");
                    }
                }

            } else {

                version = connection.getMetaData();
                System.out.println(version.getDatabaseProductName() + " " + version.getDatabaseProductVersion());

            }

            connection.close();

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
