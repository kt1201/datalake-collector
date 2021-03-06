package kr.co.penta.datalake.jdbc2seqfile;

import java.io.File;
import java.io.FileWriter;
import java.util.Base64;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import org.json.simple.JSONObject;

import kr.co.penta.datalake.common.Aes;

@SuppressWarnings("unchecked")
public class Application {

    private static String escape(String value) {

        if (value != null) {
            String escaped = "";
            int length;
            int index;
            char c;
            length = value.length();
            for (index = 0; index < length; index++) {
                c = value.charAt(index);
                if (c == 1 || c == '\\') {
                    escaped += '\\';
                }
                escaped += c;
            }
            return escaped;
        }

        return value;

    }

    private static String escapecsv(String value) {
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

    static String defaultFS(String url) {
        if (url.startsWith("hdfs://")) {
            int index;
            url = url.substring(7);
            index = url.indexOf("/");
            if (index >= 0) {
                url = url.substring(0, index);
            }
            return "hdfs://" + url;
        }
        return null;
    }

    static String tempurl(String url) {
        int index = url.lastIndexOf("/");
        if (index > 0) {
            return url.substring(0, index) + "/." + url.substring(index + 1);
        }
        return "." + url;
    }

    public static void main(String[] args) {

        String className = null;
        String url = "jdbc:sqlite::memory:";
        String username = null;
        String password = null;
        int fetchsize = 1000;
        String csvfilename = null;

        Connection connection;
        Statement statement;
        ResultSet result;
        ResultSetMetaData meta;
        int index;
        int count;

        Configuration conf = new Configuration();
        FileSystem fs = null;
        Path path = new Path(args[1]);
        Path temppath = new Path(tempurl(args[1]));
        Writer writer = null;
        String defaultFS = null;
        char delimiter = 1;
        int records = 0;

        File csvfile = null;
        FileWriter csvwriter = null;

        try {
            Text value = new Text();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            defaultFS = defaultFS(args[1]);
            if (defaultFS != null) {
                conf.set("fs.defaultFS", defaultFS);
            }
            fs = FileSystem.get(conf);
            fs.delete(temppath, false);
            writer = SequenceFile.createWriter(conf, Writer.file(temppath), Writer.keyClass(NullWritable.class),
                    Writer.valueClass(value.getClass()),
                    Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
                    Writer.replication(fs.getDefaultReplication(temppath)), Writer.blockSize(1073741824),
                    Writer.compression(SequenceFile.CompressionType.NONE, new DefaultCodec()),
                    Writer.progressable(null), Writer.metadata(new Metadata()));
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

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
        if (System.getProperty("csv") != null)
            csvfilename = System.getProperty("csv");

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

            if (csvfilename != null) {
                csvfile = new File(csvfilename);
                csvwriter = new FileWriter(csvfile);
            }

            statement = connection.createStatement();
            statement.setFetchSize(fetchsize);

            result = null;
            if (statement.execute(args[0]) == true) {
                result = statement.getResultSet();
            }

            if (result != null) {
                meta = result.getMetaData();
                count = meta.getColumnCount();
                while (result.next()) {
                    String value = "";
                    String token;
                    Object obj;
                    for (index = 1; index <= count; index++) {
                        String csvvalue = "";
                        if (index > 1) {
                            value += delimiter;
                        }
                        switch (meta.getColumnType(index)) {
                            case Types.NULL:
                                if (csvwriter != null)
                                    csvvalue = "";
                                value += "\\N";
                                break;
                            case Types.BOOLEAN:
                                obj = result.getObject(index);
                                if (obj != null) {
                                    if (result.getBoolean(index) == true) {
                                        if (csvwriter != null)
                                            csvvalue = "true";
                                        value += escape("true");
                                    } else {
                                        if (csvwriter != null)
                                            csvvalue = "false";
                                        value += escape("false");
                                    }
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                            case Types.BLOB:
                                obj = result.getObject(index);
                                if (obj != null) {
                                    token = Base64.getEncoder().encodeToString((byte[]) obj);
                                    csvvalue = "=" + token;
                                    value += token;
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                            case Types.DATE:
                                Date date = result.getDate(index);
                                if (date != null) {
                                    token = trimTimestamp(date.toString());
                                    if (csvwriter != null)
                                        csvvalue = token;
                                    value += escape(token);
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                            case Types.TIME:
                            case Types.TIME_WITH_TIMEZONE:
                                Time time = result.getTime(index);
                                if (time != null) {
                                    token = trimTimestamp(time.toString());
                                    if (csvwriter != null)
                                        csvvalue = token;
                                    value += escape(token);
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIMESTAMP_WITH_TIMEZONE:
                                Timestamp timestamp = result.getTimestamp(index);
                                if (timestamp != null) {
                                    token = trimTimestamp(timestamp.toString());
                                    if (csvwriter != null)
                                        csvvalue = token;
                                    value += escape(token);
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
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
                                    token = trimNumeric(token);
                                    if (csvwriter != null)
                                        csvvalue = token;
                                    value += escape(token);
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                            default:
                                System.err.println("컬럼의 데이터형을 찾을 수 없음: " + meta.getColumnTypeName(index));
                            case Types.CHAR:
                            case Types.VARCHAR:
                            case Types.LONGVARCHAR:
                            case Types.CLOB:
                            case Types.NCHAR:
                            case Types.NVARCHAR:
                            case Types.LONGNVARCHAR:
                            case Types.NCLOB:
                            case Types.BIT:
                                token = result.getString(index);
                                if (token != null) {
                                    if (csvwriter != null)
                                        csvvalue = escapecsv(token);
                                    value += escape(token);
                                } else {
                                    if (csvwriter != null)
                                        csvvalue = "";
                                    value += "\\N";
                                }
                                break;
                        }
                        if (csvwriter != null) {
                            csvwriter.write(csvvalue);
                            if (index < count) {
                                csvwriter.write(",");
                            } else {
                                csvwriter.write("\n");
                            }
                        }
                    }
                    writer.append(NullWritable.get(), new Text(value));
                    records++;
                }
            }

            if (csvwriter != null) {
                csvwriter.close();
                csvwriter = null;
            }
            writer.close();
            connection.close();

            fs.delete(path, false);
            if (fs.rename(temppath, path) == false) {
                System.err.println("Rename Failed: " + temppath.toString() + " => " + path.toString());
                System.exit(1);
            }

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

        JSONObject object = new JSONObject();
        object.put("Records", records);
        System.out.println(object);

    }

}
