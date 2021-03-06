package kr.co.penta.datalake.csv2seqfile;

import java.io.File;
import java.io.FileReader;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

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

        File file;
        FileReader reader;
        String token;

        int index;

        Configuration conf = new Configuration();
        FileSystem fs = null;
        Path path = new Path(args[1]);
        Path temppath = new Path(tempurl(args[1]));
        Writer writer = null;
        String defaultFS = null;
        char delimiter = 1;

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

        try {

            file = new File(args[0]);
            reader = new FileReader(file);

            do {
                String value = "";
                for (index = 1; (token = getToken(reader)) != null; index++) {
                    if (index > 1) {
                        value += delimiter;
                    }
                    if (quoted != 0) {
                        token = escape(token);
                    } else {
                        if (token.startsWith("=")) {
                            token = token.substring(1);
                        } else if (token.length() == 0) {
                            token = "\\N";
                        }
                    }
                    value += token;
                }
                if (index > 1) {
                    writer.append(NullWritable.get(), new Text(value));
                }
            } while (index > 1);

            writer.close();

            fs.delete(path, false);
            if (fs.rename(temppath, path) == false) {
                System.err.println("Rename Failed: " + temppath.toString() + " => " + path.toString());
                System.exit(1);
            }

        } catch (Exception e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
