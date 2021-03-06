package kr.co.penta.datalake.csv2jdbc;

import java.io.File;
import java.io.FileReader;
import java.lang.StringBuilder;
import java.math.BigDecimal;
import java.util.Base64;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import kr.co.penta.datalake.common.Aes;

public class Application {

    static int lastDelimiter = 0;
    static int quoted = 0;

    static int rows = 0;

    private static String getToken(FileReader reader) {

        StringBuilder result = new StringBuilder();
        int value;

        quoted = 0;

        if (lastDelimiter < 0) {
            return null;
        }
        if (lastDelimiter == '\n') {
            rows++;
            lastDelimiter = 0;
            return null;
        }

        try {

            value = reader.read();
            if (value == '\"') {
                quoted = 1;
                while (value >= 0) {
                    value = reader.read();
                    if (value == '\"') {
                        value = reader.read();
                        if (value != '\"') {
                            break;
                        }
                    }
                    result.append((char) value);
                }
            }
            while (value >= 0) {
                if (value == ',' || value == '\n') {
                    lastDelimiter = value;
                    break;
                }
                result.append((char) value);
                value = reader.read();
            }
            if (value < 0) {
                if (result.length() <= 0 && lastDelimiter == 0) {
                    lastDelimiter = value;
                    return null;
                }
                lastDelimiter = value;
            }

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

        return result.toString();

    }

    public static void main(String[] args) {

        String className = null;
        String url = "jdbc:sqlite::memory:";
        String username = null;
        String password = null;

        Connection connection;
        PreparedStatement statement;
        ParameterMetaData meta;
        int index;
        int rows;
        int added;
        int batch = 0;
        int commit = 1;

        File file;
        FileReader reader;
        String token;

        if (System.getProperty("jdbc.classname") != null)
            className = System.getProperty("jdbc.classname");
        if (System.getProperty("jdbc.url") != null)
            url = System.getProperty("jdbc.url");
        if (System.getProperty("jdbc.username") != null)
            username = System.getProperty("jdbc.username");
        if (System.getProperty("jdbc.password") != null)
            password = Aes.password(System.getProperty("jdbc.password"));
        if (System.getProperty("jdbc.batch") != null)
            batch = Integer.parseInt(System.getProperty("jdbc.batch"));
        if (System.getProperty("jdbc.commit") != null)
            commit = Integer.parseInt(System.getProperty("jdbc.commit"));

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

            file = new File(args[0]);
            reader = new FileReader(file);

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

            connection.setAutoCommit(false);

            statement = connection.prepareStatement(args[1]);
            try {
                meta = statement.getParameterMetaData();
            } catch (Exception e) {
                meta = null;
            }

            rows = 0;
            added = 0;

            for (index = 1; (token = getToken(reader)) != null; index++) {
                statement.setString(index, token);
            }
            while (index > 1) {
                if (batch > 0) {
                    statement.addBatch();
                    added++;
                } else {
                    statement.execute();
                }
                rows++;
                if (batch > 0 && added >= batch) {
                    statement.executeBatch();
                    added = 0;
                }
                if (rows >= commit) {
                    if (batch > 0 && added > 0) {
                        statement.executeBatch();
                        added = 0;
                    }
                    connection.commit();
                    rows = 0;
                }
                for (index = 1; (token = getToken(reader)) != null; index++) {
                    if (quoted != 0) {
                        statement.setString(index, token);
                    } else {
                        if (token.length() == 0 || token.equals("\\N")) {
                            if (meta != null) {
                                statement.setNull(index, meta.getParameterType(index));
                            } else {
                                statement.setNull(index, Types.NULL);
                            }
                        } else if (token.startsWith("=")) {
                            statement.setBytes(index, Base64.getDecoder().decode(token.substring(1)));
                        } else {
                            if (meta != null) {
                                switch (meta.getParameterType(index)) {
                                    case Types.CHAR:
                                    case Types.VARCHAR:
                                    case Types.LONGVARCHAR:
                                    case Types.CLOB:
                                    case Types.NCHAR:
                                    case Types.NVARCHAR:
                                    case Types.LONGNVARCHAR:
                                    case Types.NCLOB:
                                    case Types.BIT:
                                    case Types.BINARY:
                                    case Types.VARBINARY:
                                    case Types.LONGVARBINARY:
                                    case Types.BLOB:
                                        statement.setString(index, token);
                                        break;
                                    case Types.TINYINT:
                                    case Types.SMALLINT:
                                    case Types.INTEGER:
                                        statement.setInt(index, Integer.valueOf(token).intValue());
                                        break;
                                    case Types.BIGINT:
                                        statement.setLong(index, Long.valueOf(token).longValue());
                                        break;
                                    case Types.FLOAT:
                                    case Types.REAL:
                                        statement.setFloat(index, Float.valueOf(token).floatValue());
                                        break;
                                    case Types.DOUBLE:
                                        statement.setDouble(index, Double.valueOf(token).doubleValue());
                                        break;
                                    case Types.NUMERIC:
                                    case Types.DECIMAL:
                                        statement.setBigDecimal(index, new BigDecimal(token));
                                    break;
                                    case Types.DATE:
                                        statement.setDate(index, Date.valueOf(token));
                                        break;
                                    case Types.TIME:
                                    case Types.TIME_WITH_TIMEZONE:
                                        statement.setTime(index, Time.valueOf(token));
                                        break;
                                    case Types.TIMESTAMP:
                                    case Types.TIMESTAMP_WITH_TIMEZONE:
                                        statement.setTimestamp(index, Timestamp.valueOf(token));
                                        break;
                                    case Types.BOOLEAN:
                                        statement.setBoolean(index, Boolean.valueOf(token).booleanValue());
                                        break;
                                    default:
                                        System.err.println("컬럼의 데이터형을 찾을 수 없음: " + meta.getParameterType(index));
                                        statement.setString(index, token);
                                        break;
                                }
                            } else {
                                statement.setString(index, token);
                            }
                        }
                    }
                }
            }

            if (rows > 0) {
                if (batch > 0 && added > 0) {
                    statement.executeBatch();
                    added = 0;
                }
                connection.commit();
            }
            connection.close();

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
