import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileAccess {
    Configuration configuration;
    String rootPath;


    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     *                 for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        this.rootPath = rootPath;


    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException, URISyntaxException, InterruptedException {
        FileSystem hdfs = FileSystem.get(new URI(rootPath), configuration);
        Path file = new Path(hdfs.getUri() + path);


        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }
        Thread.sleep(5000);

        hdfs.create(file);
        hdfs.close();
        hdfs = null;
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException, URISyntaxException, InterruptedException {


        String oldFile = read(path);

        delete(path);

        String newFile = oldFile + content + '\n';
        FileSystem hdfs = FileSystem.get(new URI(rootPath), configuration);

        Path workPath = new Path(hdfs.getUri() + path);
        OutputStream os = hdfs.create(workPath);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));


        br.write(newFile);

        br.flush();
        br.close();
        hdfs.close();
        hdfs = null;
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException, URISyntaxException {
        FileSystem hdfs = FileSystem.get(new URI(rootPath), configuration);

        Path workPath = new Path(hdfs.getUri() + path);
        FSDataInputStream fsDataInputStream = hdfs.open(workPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String result = "";
        String line = null;
        while ((line = br.readLine()) != null) {
            result = result + line + '\n';
        }
        hdfs.close();
        hdfs = null;
        return result;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException, URISyntaxException {
        FileSystem hdfs = FileSystem.get(new URI(rootPath), configuration);

        Path workPath = new Path(hdfs.getUri() + path);

        hdfs.delete(workPath);
        hdfs = null;
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new URI(rootPath), configuration);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        Path workPath = new Path(hdfs.getUri() + path);


        return hdfs.isDirectory(workPath);

    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new URI(rootPath), configuration);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        Path workPath = new Path(hdfs.getUri() + path);


        return Arrays.stream(hdfs.listStatus(workPath)).map(k -> k.getPath().getName()).collect(Collectors.toList());
    }
}
