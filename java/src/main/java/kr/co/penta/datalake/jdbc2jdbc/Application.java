package kr.co.penta.datalake.jdbc2jdbc;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import kr.co.penta.datalake.common.Aes;

public class Application {

    public static void main(String[] args) {

        String sourceClassName = null;
        String sourceUrl = "jdbc:sqlite::memory:";
        String sourceUsername = null;
        String sourcePassword = null;
        int sourceFetchsize = 1000;
        String destinationClassName = null;
        String destinationUrl = "jdbc:sqlite::memory:";
        String destinationUsername = null;
        String destinationPassword = null;
        String destinationTruncate = null;
        int destinationBatch = 0;
        int destinationCommit = 1;

        Connection sourceConnection;
        Connection destinationConnection;
        Statement sourceStatement;
        PreparedStatement destinationStatement;
        ResultSet result;
        ResultSetMetaData meta;
        int index;
        int count;
        int rows;
        int added;

        if (System.getProperty("source.classname") != null)
            sourceClassName = System.getProperty("source.classname");
        if (System.getProperty("source.url") != null)
            sourceUrl = System.getProperty("source.url");
        if (System.getProperty("source.username") != null)
            sourceUsername = System.getProperty("source.username");
        if (System.getProperty("source.password") != null)
            sourcePassword = Aes.password(System.getProperty("source.password"));
        if (System.getProperty("source.fetchsize") != null)
            sourceFetchsize = Integer.parseInt(System.getProperty("source.fetchsize"));
        if (System.getProperty("destination.classname") != null)
            destinationClassName = System.getProperty("destination.classname");
        if (System.getProperty("destination.url") != null)
            destinationUrl = System.getProperty("destination.url");
        if (System.getProperty("destination.username") != null)
            destinationUsername = System.getProperty("destination.username");
        if (System.getProperty("destination.password") != null)
            destinationPassword = Aes.password(System.getProperty("destination.password"));
        if (System.getProperty("destination.truncate") != null)
            destinationTruncate = System.getProperty("destination.truncate");
        if (System.getProperty("destination.batch") != null)
            destinationBatch = Integer.parseInt(System.getProperty("destination.batch"));
        if (System.getProperty("destination.commit") != null)
            destinationCommit = Integer.parseInt(System.getProperty("destination.commit"));

        try {
            if (sourceClassName != null) {
                Class.forName(sourceClassName);
            }
            if (destinationClassName != null) {
                Class.forName(destinationClassName);
            }
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        try {

            int connect_retry;

            connect_retry = 0;
            do {
                try {
                    sourceConnection = DriverManager.getConnection(sourceUrl, sourceUsername, sourcePassword);
                } catch (Exception e) {
                    System.err.println(e);
                    connect_retry++;
                    if (connect_retry > 5) {
                        System.exit(1);
                    }
                    Thread.sleep(1000);
                    sourceConnection = null;
                }
            } while (sourceConnection == null);

            connect_retry = 0;
            do {
                try {
                    destinationConnection = DriverManager.getConnection(destinationUrl, destinationUsername,
                            destinationPassword);
                } catch (Exception e) {
                    System.err.println(e);
                    connect_retry++;
                    if (connect_retry > 5) {
                        System.exit(1);
                    }
                    Thread.sleep(1000);
                    destinationConnection = null;
                }
            } while (destinationConnection == null);
            destinationConnection.setAutoCommit(false);

            sourceStatement = sourceConnection.createStatement();
            sourceStatement.setFetchSize(sourceFetchsize);
            destinationStatement = destinationConnection.prepareStatement(args[1]);

            result = null;
            if (sourceStatement.execute(args[0]) == true) {
                result = sourceStatement.getResultSet();
            }

            rows = 0;
            added = 0;

            if (result != null) {
                if (destinationTruncate != null) {
                    Statement truncateStatement = destinationConnection.createStatement();
                    truncateStatement.executeUpdate(destinationTruncate);
                }
                meta = result.getMetaData();
                count = meta.getColumnCount();
                while (result.next()) {
                    destinationStatement.clearParameters();
                    for (index = 1; index <= count; index++) {
                        switch (meta.getColumnType(index)) {
                            case Types.CHAR:
                            case Types.VARCHAR:
                            case Types.LONGVARCHAR:
                            case Types.CLOB:
                            case Types.NCHAR:
                            case Types.NVARCHAR:
                            case Types.LONGNVARCHAR:
                            case Types.NCLOB:
                            case Types.BIT:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setString(index, result.getString(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                            case Types.BLOB:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setBytes(index, result.getBytes(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                            case Types.BIGINT:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setLong(index, result.getLong(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setDouble(index, result.getDouble(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setBigDecimal(index, result.getBigDecimal(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.DATE:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setDate(index, result.getDate(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.TIME:
                            case Types.TIME_WITH_TIMEZONE:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setTime(index, result.getTime(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIMESTAMP_WITH_TIMEZONE:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setTimestamp(index, result.getTimestamp(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.BOOLEAN:
                                if (result.getObject(index) != null) {
                                    destinationStatement.setBoolean(index, result.getBoolean(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                            case Types.NULL:
                                destinationStatement.setNull(index, meta.getColumnType(index));
                                break;
                            default:
                                System.err.println("컬럼의 데이터형을 찾을 수 없음: " + meta.getColumnTypeName(index));
                                if (result.getObject(index) != null) {
                                    destinationStatement.setString(index, result.getString(index));
                                } else {
                                    destinationStatement.setNull(index, meta.getColumnType(index));
                                }
                                break;
                        }
                    }
                    if (destinationBatch > 0) {
                        destinationStatement.addBatch();
                        added++;
                    } else {
                        destinationStatement.execute();
                    }
                    rows++;
                    if (destinationBatch > 0 && added >= destinationBatch) {
                        destinationStatement.executeBatch();
                        added = 0;
                    }
                    if (rows >= destinationCommit) {
                        if (destinationBatch > 0 && added > 0) {
                            destinationStatement.executeBatch();
                            added = 0;
                        }
                        destinationConnection.commit();
                        rows = 0;
                    }
                }
            }

            if (rows > 0) {
                if (destinationBatch > 0 && added > 0) {
                    destinationStatement.executeBatch();
                    added = 0;
                }
                destinationConnection.commit();
            }
            destinationConnection.close();
            sourceConnection.close();

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
