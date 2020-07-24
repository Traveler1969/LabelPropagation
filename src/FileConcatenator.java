import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * FileConcatenator的任务是将给定的HDFS目录下的所有文件拼接为一个文件，
 * 它被用于将LabelViewer的输出结果拼接起来，以便将该文件通过Distributed Cache分发到各个结点，
 * args[0]指定输入目录，args[1]指定输出文件路径
 */
public class FileConcatenator {
    public static void main(String[] args) throws Exception {
        // 加载配置文件，获取HDFS文件系统
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        // 获取指定目录下的文件列表
        Path inputPath = new Path(args[0]);
        FileStatus[] status = hdfs.listStatus(inputPath);
        // 创建输出文件，并在文件上打开一个PrintWriter
        Path outputPath = new Path(args[1]);
        FSDataOutputStream outputStream = hdfs.create(outputPath);
        PrintWriter pw = new PrintWriter(outputStream);
        // 遍历输入目录下的所有文件，执行文件拼接
        for(FileStatus curStatus : status) {
            FSDataInputStream inputStream = hdfs.open(curStatus.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String inputLine = br.readLine();
            while(inputLine != null) {
                pw.println(inputLine);
                inputLine = br.readLine();
            }
            br.close();
        }
        pw.close(); // 不需要先调用pw.flush()，底层的OutputStreamWriter在关闭时会flush
    }
}
