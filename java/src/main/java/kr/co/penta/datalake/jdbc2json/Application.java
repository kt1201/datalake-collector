package kr.co.penta.datalake.jdbc2json;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.Collection;
import java.util.HashMap;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import kr.co.penta.datalake.common.Aes;

@SuppressWarnings("unchecked")
public class Application {

    static int fetchsize = 1000;

    static HashMap<Long, Item> map = new HashMap<Long, Item>();
    static Long lastid = Long.valueOf(0);

    public static class Item {
        String query;
        PreparedStatement statement;
        ResultSet resultset;
        boolean csv;

        Item() {
            this.query = null;
            this.statement = null;
            this.resultset = null;
            this.csv = false;
        }
    }

    private static void clear() {
        Collection<Item> values = map.values();
        for (Item value : values) {
            if (value.statement != null) {
                value.statement = null;
            }
        }
    }

    private static void binds(Connection connection, PreparedStatement statement, JSONArray binds) throws Exception {

        int index;
        int count;

        ParameterMetaData paramMeta = null;

        statement.clearParameters();

        try {
            paramMeta = statement.getParameterMetaData();
        } catch (Exception e) {
            paramMeta = null;
        }

        if (binds != null) {
            count = binds.size();
            for (index = 0; index < count; index++) {
                Object obj = binds.get(index);
                if (obj == null) {
                    if (paramMeta != null) {
                        statement.setNull(index + 1, paramMeta.getParameterType(index + 1));
                    } else {
                        statement.setString(index + 1, null);
                    }
                } else if (obj.getClass() == Class.forName("java.lang.String")) {
                    statement.setString(index + 1, (String) obj);
                } else if (obj.getClass() == Class.forName("java.lang.Long")) {
                    statement.setLong(index + 1, (Long) obj);
                } else if (obj.getClass() == Class.forName("java.lang.Double")) {
                    statement.setDouble(index + 1, (Double) obj);
                } else if (obj.getClass() == Class.forName("java.lang.Boolean")) {
                    statement.setBoolean(index + 1, (Boolean) obj);
                } else {
                    throw new Exception("클래스를 처리할 수 없음: " + obj.getClass());
                }
            }
        }

    }

    private static Boolean records(ResultSet resultset, JSONArray array, int fetchsize, Boolean meta) throws Exception {

        int index;
        int count;

        ResultSetMetaData metadata;
        int type;
        String name;
        boolean result;

        metadata = resultset.getMetaData();
        count = metadata.getColumnCount();
        if (meta == Boolean.TRUE) {
            JSONArray columns = new JSONArray();
            for (index = 1; index <= count; index++) {
                JSONObject item = new JSONObject();
                try {
                    item.put("CatalogName", metadata.getCatalogName(index));
                } catch (Exception e) {
                    item.put("CatalogName", null);
                }
                try {
                    item.put("ColumnClassName", metadata.getColumnClassName(index));
                } catch (Exception e) {
                    item.put("ColumnClassName", null);
                }
                try {
                    item.put("ColumnDisplaySize", metadata.getColumnDisplaySize(index));
                } catch (Exception e) {
                    item.put("ColumnDisplaySize", null);
                }
                try {
                    item.put("ColumnLabel", metadata.getColumnLabel(index).toUpperCase());
                } catch (Exception e) {
                    item.put("ColumnLabel", null);
                }
                try {
                    item.put("ColumnName", metadata.getColumnName(index));
                } catch (Exception e) {
                    item.put("ColumnName", null);
                }
                try {
                    item.put("ColumnType", metadata.getColumnType(index));
                } catch (Exception e) {
                    item.put("ColumnType", null);
                }
                try {
                    item.put("ColumnTypeName", metadata.getColumnTypeName(index));
                } catch (Exception e) {
                    item.put("ColumnTypeName", null);
                }
                try {
                    item.put("Precision", metadata.getPrecision(index));
                } catch (Exception e) {
                    item.put("Precision", null);
                }
                try {
                    item.put("Scale", metadata.getScale(index));
                } catch (Exception e) {
                    item.put("Scale", null);
                }
                try {
                    item.put("SchemaName", metadata.getSchemaName(index));
                } catch (Exception e) {
                    item.put("SchemaName", null);
                }
                try {
                    item.put("TableName", metadata.getTableName(index));
                } catch (Exception e) {
                    item.put("TableName", null);
                }
                try {
                    item.put("AutoIncrement", metadata.isAutoIncrement(index));
                } catch (Exception e) {
                    item.put("AutoIncrement", null);
                }
                try {
                    item.put("CaseSensitive", metadata.isCaseSensitive(index));
                } catch (Exception e) {
                    item.put("CaseSensitive", null);
                }
                try {
                    item.put("Currency", metadata.isCurrency(index));
                } catch (Exception e) {
                    item.put("Currency", null);
                }
                try {
                    item.put("DefinitelyWritable", metadata.isDefinitelyWritable(index));
                } catch (Exception e) {
                    item.put("DefinitelyWritable", null);
                }
                try {
                    item.put("Nullable", metadata.isNullable(index));
                } catch (Exception e) {
                    item.put("Nullable", null);
                }
                try {
                    item.put("ReadOnly", metadata.isReadOnly(index));
                } catch (Exception e) {
                    item.put("ReadOnly", null);
                }
                try {
                    item.put("Searchable", metadata.isSearchable(index));
                } catch (Exception e) {
                    item.put("Searchable", null);
                }
                try {
                    item.put("Signed", metadata.isSigned(index));
                } catch (Exception e) {
                    item.put("Signed", null);
                }
                try {
                    item.put("Writable", metadata.isWritable(index));
                } catch (Exception e) {
                    item.put("Writable", null);
                }
                columns.add(item);
            }
            array.add(columns);
        }
        while ((result = resultset.next())) {
            JSONObject record = new JSONObject();
            for (index = 1; index <= count; index++) {
                type = metadata.getColumnType(index);
                name = metadata.getColumnLabel(index).toUpperCase();
                switch (type) {
                    case Types.NULL:
                        record.put(name, null);
                        break;
                    case Types.BOOLEAN:
                        if (resultset.getObject(index) != null) {
                            record.put(name, resultset.getBoolean(index));
                        } else {
                            record.put(name, null);
                        }
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        if (resultset.getObject(index) != null) {
                            record.put(name, resultset.getLong(index));
                        } else {
                            record.put(name, null);
                        }
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        if (resultset.getObject(index) != null) {
                            record.put(name, resultset.getDouble(index));
                        } else {
                            record.put(name, null);
                        }
                        break;
                    default:
                        System.err.println("Unknown Type: name = " + name + " (" + type + ") ");
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                    case Types.BIT:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        record.put(name, resultset.getString(index));
                        break;
                }
            }
            array.add(record);
            if (fetchsize > 0) {
                fetchsize--;
                if (fetchsize == 0) {
                    break;
                }
            }
        }

        return Boolean.valueOf(result);

    }

    private static Boolean arrays(ResultSet resultset, JSONArray array, int fetchsize, Boolean header, Boolean meta)
            throws Exception {

        int index;
        int count;

        ResultSetMetaData metadata;
        int type;
        String name;
        boolean result;

        metadata = resultset.getMetaData();
        count = metadata.getColumnCount();
        if (meta == Boolean.TRUE) {
            JSONArray columns = new JSONArray();
            for (index = 1; index <= count; index++) {
                JSONObject item = new JSONObject();
                try {
                    item.put("CatalogName", metadata.getCatalogName(index));
                } catch (Exception e) {
                    item.put("CatalogName", null);
                }
                try {
                    item.put("ColumnClassName", metadata.getColumnClassName(index));
                } catch (Exception e) {
                    item.put("ColumnClassName", null);
                }
                try {
                    item.put("ColumnDisplaySize", metadata.getColumnDisplaySize(index));
                } catch (Exception e) {
                    item.put("ColumnDisplaySize", null);
                }
                try {
                    item.put("ColumnLabel", metadata.getColumnLabel(index).toUpperCase());
                } catch (Exception e) {
                    item.put("ColumnLabel", null);
                }
                try {
                    item.put("ColumnName", metadata.getColumnName(index));
                } catch (Exception e) {
                    item.put("ColumnName", null);
                }
                try {
                    item.put("ColumnType", metadata.getColumnType(index));
                } catch (Exception e) {
                    item.put("ColumnType", null);
                }
                try {
                    item.put("ColumnTypeName", metadata.getColumnTypeName(index));
                } catch (Exception e) {
                    item.put("ColumnTypeName", null);
                }
                try {
                    item.put("Precision", metadata.getPrecision(index));
                } catch (Exception e) {
                    item.put("Precision", null);
                }
                try {
                    item.put("Scale", metadata.getScale(index));
                } catch (Exception e) {
                    item.put("Scale", null);
                }
                try {
                    item.put("SchemaName", metadata.getSchemaName(index));
                } catch (Exception e) {
                    item.put("SchemaName", null);
                }
                try {
                    item.put("TableName", metadata.getTableName(index));
                } catch (Exception e) {
                    item.put("TableName", null);
                }
                try {
                    item.put("AutoIncrement", metadata.isAutoIncrement(index));
                } catch (Exception e) {
                    item.put("AutoIncrement", null);
                }
                try {
                    item.put("CaseSensitive", metadata.isCaseSensitive(index));
                } catch (Exception e) {
                    item.put("CaseSensitive", null);
                }
                try {
                    item.put("Currency", metadata.isCurrency(index));
                } catch (Exception e) {
                    item.put("Currency", null);
                }
                try {
                    item.put("DefinitelyWritable", metadata.isDefinitelyWritable(index));
                } catch (Exception e) {
                    item.put("DefinitelyWritable", null);
                }
                try {
                    item.put("Nullable", metadata.isNullable(index));
                } catch (Exception e) {
                    item.put("Nullable", null);
                }
                try {
                    item.put("ReadOnly", metadata.isReadOnly(index));
                } catch (Exception e) {
                    item.put("ReadOnly", null);
                }
                try {
                    item.put("Searchable", metadata.isSearchable(index));
                } catch (Exception e) {
                    item.put("Searchable", null);
                }
                try {
                    item.put("Signed", metadata.isSigned(index));
                } catch (Exception e) {
                    item.put("Signed", null);
                }
                try {
                    item.put("Writable", metadata.isWritable(index));
                } catch (Exception e) {
                    item.put("Writable", null);
                }
                columns.add(item);
            }
            array.add(columns);
        }
        if (header == Boolean.TRUE) {
            JSONArray record = new JSONArray();
            for (index = 1; index <= count; index++) {
                name = metadata.getColumnLabel(index).toUpperCase();
                record.add(name);
            }
            array.add(record);
        }
        while ((result = resultset.next())) {
            JSONArray record = new JSONArray();
            for (index = 1; index <= count; index++) {
                type = metadata.getColumnType(index);
                name = metadata.getColumnLabel(index).toUpperCase();
                switch (type) {
                    case Types.NULL:
                        record.add(null);
                        break;
                    case Types.BOOLEAN:
                        if (resultset.getObject(index) != null) {
                            record.add(resultset.getBoolean(index));
                        } else {
                            record.add(null);
                        }
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        if (resultset.getObject(index) != null) {
                            record.add(resultset.getLong(index));
                        } else {
                            record.add(null);
                        }
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        if (resultset.getObject(index) != null) {
                            record.add(resultset.getDouble(index));
                        } else {
                            record.add(null);
                        }
                        break;
                    default:
                        System.err.println("Unknown Type: name = " + name + " (" + type + ") ");
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                    case Types.BIT:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        record.add(resultset.getString(index));
                        break;
                }
            }
            array.add(record);
            if (fetchsize > 0) {
                fetchsize--;
                if (fetchsize == 0) {
                    break;
                }
            }
        }

        return Boolean.valueOf(result);

    }

    private static void check(Connection connection) {

        try {

            JSONObject object = new JSONObject();
            object.put("Valid", connection.isValid(10));
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }
    }

    private static void execute(Connection connection, String query, Long id, JSONArray binds, Boolean csv,
            Boolean header, Boolean meta) {

        try {

            PreparedStatement statement = null;
            ResultSet resultset = null;

            if (query != null) {
                statement = connection.prepareStatement(query);
            } else {
                Item value = map.get(id);
                if (value.statement == null) {
                    value.statement = connection.prepareStatement(value.query);
                }
                statement = value.statement;
            }

            statement.setFetchSize(fetchsize);

            binds(connection, statement, binds);

            if (statement.execute() == true) {
                resultset = statement.getResultSet();
            }

            if (resultset != null) {

                JSONArray array = new JSONArray();
                if (csv == Boolean.TRUE) {
                    arrays(resultset, array, 0, header, meta);
                } else {
                    records(resultset, array, 0, meta);
                }
                System.out.println(array);

            } else {

                JSONObject object = new JSONObject();
                object.put("UpdateCount", statement.getUpdateCount());
                System.out.println(object);

            }

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void executeList(Connection connection, String query, Long id, JSONArray binds) {

        try {

            PreparedStatement statement = null;

            if (query != null) {
                statement = connection.prepareStatement(query);
            } else {
                Item value = map.get(id);
                if (value.statement == null) {
                    value.statement = connection.prepareStatement(value.query);
                }
                statement = value.statement;
            }

            statement.setFetchSize(fetchsize);

            for (int index = 0; index < binds.size(); index++) {
                binds(connection, statement, (JSONArray) binds.get(index));
                statement.addBatch();
            }

            int[] updateCounts = statement.executeBatch();

            JSONObject object = new JSONObject();
            JSONArray array = new JSONArray();
            for (int updateCount : updateCounts) {
                array.add(updateCount);
            }
            object.put("UpdateCount", array);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void statement(Connection connection, String query) {

        try {

            Item value = new Item();
            lastid++;
            value.query = query;
            value.statement = connection.prepareStatement(query);
            map.put(lastid, value);

            JSONObject object = new JSONObject();
            object.put("id", lastid);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }
    }

    private static void free(Connection connection, Long id) {

        try {

            map.remove(id);

            JSONObject object = new JSONObject();
            object.put("id", id);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void open(Connection connection, Long id, JSONArray binds, Boolean csv, Boolean header,
            Boolean meta) {

        try {

            Item statement = map.get(id);
            Item value = new Item();
            lastid++;

            binds(connection, statement.statement, binds);
            value.resultset = statement.statement.executeQuery();
            value.csv = csv.booleanValue();
            JSONArray array = new JSONArray();
            if (value.csv == Boolean.TRUE) {
                if (arrays(value.resultset, array, fetchsize, header, meta) == false) {
                    value.resultset = null;
                }
            } else {
                if (records(value.resultset, array, fetchsize, meta) == false) {
                    value.resultset = null;
                }
            }
            map.put(lastid, value);

            JSONObject object = new JSONObject();
            object.put("id", lastid);
            object.put("result", array);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }
    }

    private static void fetch(Connection connection, Long id) {

        try {

            Item value = map.get(id);
            JSONArray array = new JSONArray();
            if (value.resultset != null) {
                if (value.csv == Boolean.TRUE) {
                    if (arrays(value.resultset, array, fetchsize, Boolean.FALSE, Boolean.FALSE) == false) {
                        value.resultset = null;
                    }
                } else {
                    if (records(value.resultset, array, fetchsize, Boolean.FALSE) == false) {
                        value.resultset = null;
                    }
                }
            }

            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }
    }

    private static void close(Connection connection, Long id) {

        try {

            map.remove(id);

            JSONObject object = new JSONObject();
            object.put("id", id);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void getautocommit(Connection connection) {

        try {

            JSONObject object = new JSONObject();
            object.put("autocommit", connection.getAutoCommit());
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void setautocommit(Connection connection, Boolean autocommit) {

        try {

            JSONObject object = new JSONObject();
            connection.setAutoCommit(autocommit);
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void commit(Connection connection) {

        try {

            JSONObject object = new JSONObject();
            connection.commit();
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void rollback(Connection connection) {

        try {

            JSONObject object = new JSONObject();
            connection.rollback();
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void database(Connection connection) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            JSONObject object = new JSONObject();
            object.put("DatabaseMajorVersion", metadata.getDatabaseMajorVersion());
            object.put("DatabaseMinorVersion", metadata.getDatabaseMinorVersion());
            object.put("DatabaseProductName", metadata.getDatabaseProductName());
            object.put("DatabaseProductVersion", metadata.getDatabaseProductVersion());
            object.put("DriverMajorVersion", metadata.getDriverMajorVersion());
            object.put("DriverMinorVersion", metadata.getDriverMinorVersion());
            object.put("DriverName", metadata.getDriverName());
            object.put("DriverVersion", metadata.getDriverVersion());
            object.put("JDBCMajorVersion", metadata.getJDBCMajorVersion());
            object.put("JDBCMinorVersion", metadata.getJDBCMinorVersion());
            object.put("CatalogTerm", metadata.getCatalogTerm());
            object.put("SchemaTerm", metadata.getSchemaTerm());
            object.put("CatalogSeparator", metadata.getCatalogSeparator());
            object.put("IdentifierQuoteString", metadata.getIdentifierQuoteString());
            object.put("SearchStringEscape", metadata.getSearchStringEscape());
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void username(Connection connection) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            JSONObject object = new JSONObject();
            object.put("UserName", metadata.getUserName());
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void catalog(Connection connection) {

        try {

            JSONObject object = new JSONObject();
            object.put("Catalog", connection.getCatalog());
            System.out.println(object);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void catalogs(Connection connection) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            JSONArray records = new JSONArray();
            JSONArray record;
            resultset = metadata.getCatalogs();
            arrays(resultset, records, 0, Boolean.FALSE, Boolean.FALSE);
            for (int index = 0; index < records.size(); index++) {
                record = (JSONArray) records.get(index);
                array.add((String) record.get(0));
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void schemas(Connection connection, String catalog, String schema, Boolean csv, Boolean header,
            Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            if (catalog != null || schema != null) {
                resultset = metadata.getSchemas(catalog, schema);
            } else {
                resultset = metadata.getSchemas();
            }
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void tables(Connection connection, String catalog, String schema, String table, JSONArray types,
            Boolean csv, Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            String[] datatypes = null;
            if (types != null) {
                datatypes = new String[types.size()];
                for (int index = 0; index < types.size(); index++) {
                    datatypes[index] = (String) types.get(index);
                }
            }
            resultset = metadata.getTables(catalog, schema, table, datatypes);
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void columns(Connection connection, String catalog, String schema, String table, String column,
            Boolean csv, Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            resultset = metadata.getColumns(catalog, schema, table, column);
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void indices(Connection connection, String catalog, String schema, String table, Boolean unique,
            Boolean approximate, Boolean csv, Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            resultset = metadata.getIndexInfo(catalog, schema, table, unique.booleanValue(),
                    approximate.booleanValue());
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void primarykeys(Connection connection, String catalog, String schema, String table, Boolean csv,
            Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            resultset = metadata.getPrimaryKeys(catalog, schema, table);
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void exportedkeys(Connection connection, String catalog, String schema, String table, Boolean csv,
            Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            resultset = metadata.getExportedKeys(catalog, schema, table);
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void importedkeys(Connection connection, String catalog, String schema, String table, Boolean csv,
            Boolean header, Boolean meta) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            resultset = metadata.getImportedKeys(catalog, schema, table);
            if (csv == Boolean.TRUE) {
                arrays(resultset, array, 0, header, meta);
            } else {
                records(resultset, array, 0, meta);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void keywords(Connection connection) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            JSONArray array = new JSONArray();
            String[] keywords = metadata.getSQLKeywords().split(",");
            for (String keyword : keywords) {
                array.add(keyword);
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    private static void tabletypes(Connection connection) {

        try {

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultset = null;
            JSONArray array = new JSONArray();
            JSONArray records = new JSONArray();
            JSONArray record;
            resultset = metadata.getTableTypes();
            arrays(resultset, records, 0, Boolean.FALSE, Boolean.FALSE);
            for (int index = 0; index < records.size(); index++) {
                record = (JSONArray) records.get(index);
                array.add((String) record.get(0));
            }
            System.out.println(array);

        } catch (Exception e) {

            JSONObject object = new JSONObject();
            object.put("Exception", e.toString());
            System.out.println(object);

        }

    }

    public static void main(String[] args) {

        String className = null;
        String url = "jdbc:sqlite::memory:";
        String username = null;
        String password = null;
        String check = null;

        Connection connection = null;
        PreparedStatement checkStatement = null;
        long checkedTimeMillis;

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
        if (System.getProperty("jdbc.check") != null)
            check = System.getProperty("jdbc.check");

        try {
            if (className != null) {
                Class.forName(className);
            }
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        try {

            String json;
            int connect_retry = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            do {
                try {
                    connection = DriverManager.getConnection(url, username, password);
                    if (check != null) {
                        checkStatement = connection.prepareStatement(check);
                        checkStatement.execute();
                    }
                } catch (Exception e) {
                    System.err.println(e);
                    connect_retry++;
                    if (connect_retry > 5) {
                        System.exit(1);
                    }
                    Thread.sleep(1000);
                    if (checkStatement != null) {
                        checkStatement.close();
                        checkStatement = null;
                    }
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                }
            } while (connection == null);
            checkedTimeMillis = System.currentTimeMillis();

            if (args.length > 0) {

                JSONArray binds = null;
                if (args.length >= 2) {
                    JSONParser parser = new JSONParser();
                    binds = (JSONArray) parser.parse(args[1]);
                }
                execute(connection, args[0], Long.valueOf(0), binds, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);

            } else {

                while ((json = reader.readLine()) != null) {

                    JSONParser parser = new JSONParser();

                    JSONObject request = (JSONObject) parser.parse(json);
                    String type = (String) request.get("type");
                    String query = (String) request.get("query");
                    String catalog = (String) request.get("catalog");
                    String schema = (String) request.get("schema");
                    String table = (String) request.get("table");
                    String column = (String) request.get("column");
                    Long id = (Long) request.get("id");
                    Boolean unique = (Boolean) request.get("unique");
                    Boolean approximate = (Boolean) request.get("approximate");
                    Boolean csv = (Boolean) request.get("csv");
                    Boolean header = (Boolean) request.get("header");
                    Boolean meta = (Boolean) request.get("meta");
                    Boolean autocommit = (Boolean) request.get("autocommit");
                    JSONArray types = (JSONArray) request.get("types");
                    JSONArray binds = (JSONArray) request.get("binds");

                    if (type == null) {
                        type = "null";
                    }
                    if (unique == null) {
                        unique = Boolean.FALSE;
                    }
                    if (approximate == null) {
                        approximate = Boolean.FALSE;
                    }
                    if (csv == null) {
                        csv = Boolean.FALSE;
                    }
                    if (header == null) {
                        header = Boolean.TRUE;
                    }
                    if (meta == null) {
                        meta = Boolean.TRUE;
                    }
                    if (autocommit == null) {
                        autocommit = Boolean.TRUE;
                    }

                    if (connection != null) {
                        if (connection.isClosed() == true) {
                            try {
                                if (checkStatement != null) {
                                    checkStatement.close();
                                    checkStatement = null;
                                }
                                clear();
                                connection.close();
                                connection = null;
                            } catch (Exception e) {
                                System.err.println(e);
                                checkStatement = null;
                                connection = null;
                            }
                        } else {
                            long now = System.currentTimeMillis();
                            if (now - checkedTimeMillis > 60000 || now < checkedTimeMillis) {
                                try {
                                    if (checkStatement != null) {
                                        checkStatement.execute();
                                    } else if (connection.isValid(10) != true) {
                                        throw new Exception("Connection Invalid");
                                    }
                                    checkedTimeMillis = now;
                                } catch (Exception e) {
                                    System.err.println(e);
                                    if (checkStatement != null) {
                                        checkStatement.close();
                                        checkStatement = null;
                                    }
                                    clear();
                                    if (connection != null) {
                                        connection.close();
                                        connection = null;
                                    }
                                }
                            }
                        }
                    }
                    if (connection == null) {
                        connect_retry = 0;
                        do {
                            try {
                                connection = DriverManager.getConnection(url, username, password);
                                if (check != null) {
                                    checkStatement = connection.prepareStatement(check);
                                    checkStatement.execute();
                                }
                            } catch (Exception e) {
                                System.err.println(e);
                                connect_retry++;
                                if (connect_retry > 5) {
                                    System.exit(1);
                                }
                                Thread.sleep(1000);
                                if (checkStatement != null) {
                                    checkStatement.close();
                                    checkStatement = null;
                                }
                                if (connection != null) {
                                    connection.close();
                                    connection = null;
                                }
                            }
                        } while (connection == null);
                        checkedTimeMillis = System.currentTimeMillis();
                    }

                    switch (type) {
                        case "check":
                            check(connection);
                            break;
                        case "execute":
                            execute(connection, query, id, binds, csv, header, meta);
                            break;
                        case "execute_list":
                            executeList(connection, query, id, binds);
                            break;
                        case "statement":
                            statement(connection, query);
                            break;
                        case "free":
                            free(connection, id);
                            break;
                        case "open":
                            open(connection, id, binds, csv, header, meta);
                            break;
                        case "fetch":
                            fetch(connection, id);
                            break;
                        case "close":
                            close(connection, id);
                            break;
                        case "getautocommit":
                            getautocommit(connection);
                            break;
                        case "setautocommit":
                            setautocommit(connection, autocommit);
                            break;
                        case "commit":
                            commit(connection);
                            break;
                        case "rollback":
                            rollback(connection);
                            break;
                        case "database":
                            database(connection);
                            break;
                        case "username":
                            username(connection);
                            break;
                        case "catalog":
                            catalog(connection);
                            break;
                        case "catalogs":
                            catalogs(connection);
                            break;
                        case "schemas":
                            schemas(connection, catalog, schema, csv, header, meta);
                            break;
                        case "tables":
                            tables(connection, catalog, schema, table, types, csv, header, meta);
                            break;
                        case "columns":
                            columns(connection, catalog, schema, table, column, csv, header, meta);
                            break;
                        case "indices":
                            indices(connection, catalog, schema, table, unique, approximate, csv, header, meta);
                            break;
                        case "primarykeys":
                            primarykeys(connection, catalog, schema, table, csv, header, meta);
                            break;
                        case "importedkeys":
                            importedkeys(connection, catalog, schema, table, csv, header, meta);
                            break;
                        case "exportedkeys":
                            exportedkeys(connection, catalog, schema, table, csv, header, meta);
                            break;
                        case "keywords":
                            keywords(connection);
                            break;
                        case "tabletypes":
                            tabletypes(connection);
                            break;
                        case "disconnect":
                            clear();
                            connection.close();
                            connection = null;
                            JSONObject object1 = new JSONObject();
                            System.out.println(object1);
                            break;
                        default:
                            JSONObject object2 = new JSONObject();
                            object2.put("Exception", "알 수 없는 유형: " + type);
                            System.out.println(object2);
                            break;
                    }

                }

            }

            if (connection != null) {
                if (connection.isClosed() == false) {
                    connection.close();
                }
                connection = null;
            }

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
