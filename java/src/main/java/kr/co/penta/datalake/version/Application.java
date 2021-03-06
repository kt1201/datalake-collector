package kr.co.penta.datalake.version;

import java.util.Base64;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import kr.co.penta.datalake.common.Aes;

public class Application {

    private static String escape(String value) {
        int index;
        int length;
        String escaped = "";
        char c;
        escaped += "\"";
        if (value != null) {
            length = value.length();
            for (index = 0; index < length; index++) {
                c = value.charAt(index);
                if (c == '\"') {
                    escaped += "\"\"";
                } else {
                    escaped += c;
                }
            }
        }
        escaped += "\"";
        return escaped;
    }

    private static String trimTimestamp(String value) {
        int end;
        int index;
        for (end = value.length() - 1; end >= 0; end--) {
            if (value.charAt(end) != '0') {
                break;
            }
        }
        for (index = end; index >= 0; index--) {
            if (value.charAt(index) == '.') {
                break;
            }
        }
        if (index >= 0) {
            if (end == index) {
                end++;
            }
            value = value.substring(0, end + 1);
        }
        return value;
    }

    private static String trimNumeric(String value) {
        int end;
        int index;
        char c = '\0';
        if (value.startsWith("0E") == true || value.startsWith("0e") == true) {
            value = "0";
        } else {
            if (value.startsWith(".") == true) {
                value = "0" + value;
            } else if (value.startsWith("-.") == true) {
                value = "-0" + value.substring(1);
            }
            for (end = value.length() - 1; end >= 0; end--) {
                c = value.charAt(end);
                if (c != '0') {
                    break;
                }
            }
            for (index = end; index >= 0; index--) {
                c = value.charAt(index);
                if (c < '0' || c > '9') {
                    break;
                }
            }
            if (index >= 0 && c == '.') {
                if (end == index) {
                    end = index - 1;
                }
                value = value.substring(0, end + 1);
            }
        }
        return value;
    }

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
                        System.out.print(escape(meta.getColumnName(index).toUpperCase()));
                        if (index < count) {
                            System.out.print(",");
                        } else {
                            System.out.print("\n");
                        }
                    }
                    while (result.next()) {
                        for (index = 1; index <= count; index++) {
                            Object obj;
                            String token;
                            String value = "";
                            switch (meta.getColumnType(index)) {
                                case Types.BINARY:
                                case Types.VARBINARY:
                                case Types.LONGVARBINARY:
                                case Types.BLOB:
                                    obj = result.getObject(index);
                                    if (obj != null) {
                                        value = "=" + Base64.getEncoder().encodeToString((byte[]) obj);
                                    } else {
                                        value = "";
                                    }
                                    break;
                                case Types.DATE:
                                    Date date = result.getDate(index);
                                    if (date != null) {
                                        value = trimTimestamp(date.toString());
                                    } else {
                                        value = "";
                                    }
                                    break;
                                case Types.TIME:
                                case Types.TIME_WITH_TIMEZONE:
                                    Time time = result.getTime(index);
                                    if (time != null) {
                                        value = trimTimestamp(time.toString());
                                    } else {
                                        value = "";
                                    }
                                    break;
                                case Types.TIMESTAMP:
                                case Types.TIMESTAMP_WITH_TIMEZONE:
                                    Timestamp timestamp = result.getTimestamp(index);
                                    if (timestamp != null) {
                                        value = trimTimestamp(timestamp.toString());
                                    } else {
                                        value = "";
                                    }
                                    break;
                                case Types.NUMERIC:
                                case Types.DECIMAL:
                                case Types.FLOAT:
                                case Types.REAL:
                                case Types.DOUBLE:
                                case Types.TINYINT:
                                case Types.SMALLINT:
                                case Types.INTEGER:
                                case Types.BIGINT:
                                    token = result.getString(index);
                                    if (token != null) {
                                        value = trimNumeric(token);
                                    } else {
                                        value = "";
                                    }
                                    break;
                                default:
                                    token = result.getString(index);
                                    if (token != null) {
                                        value = escape(token);
                                    } else {
                                        value = "";
                                    }
                                    break;
                            }
                            System.out.print(value);
                            if (index < count) {
                                System.out.print(",");
                            } else {
                                System.out.print("\n");
                            }
                        }
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
