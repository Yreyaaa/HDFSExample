import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
//        Configuration configuration = new Configuration();
//        configuration.set("dfs.client.use.datanode.hostname", "true");
//        System.setProperty("HADOOP_USER_NAME", "root");
//
//        FileSystem hdfs = FileSystem.get(
//            new URI("hdfs://8e8c8d5b8070:8020"), configuration
//        );
//        Path file = new Path("hdfs://8e8c8d5b8070:8020/test/file.txt");
//
//
//        if (hdfs.exists(file)) {
//            hdfs.delete(file, true);
//        }
//
//        OutputStream os = hdfs.create(file);
//        BufferedWriter br = new BufferedWriter(
//            new OutputStreamWriter(os, "UTF-8")
//        );
//
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < 1_000_000; i++) {
            stringBuilder.append(getRandomWord() + " ");
        }
//
//        br.flush();
//        br.close();
//        hdfs.close();

        FileAccess fileAccess = new FileAccess("hdfs://a5ddfa9fc6f9:8020");

       fileAccess.create("/MyDirectory/MyFile.txt");

       fileAccess.append("/MyDirectory/MyFile.txt", stringBuilder.toString());

      //  System.out.println(fileAccess.read("/MyDirectory/MyFile.txt"));

       // fileAccess.delete("/user");

        //System.out.println(fileAccess.isDirectory("/MyDirectory"));

       // fileAccess.list("/MyDirectory").forEach(System.out::println);


        System.out.println();
    }



    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
