import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class LabelNovelCounter {
    public static void main(String[] args) throws Exception {
        // 创建作业，设置作业的基本信息，将TextOutputFormat的分隔符设置为空格
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "label novel count");
        job.setJarByClass(LabelNovelCounter.class);
        // 从命令行参数中获取原数据路径、拼接文件路径、输出路径
        Path novelPath = new Path(args[0]);
        String concatenatedFilePath = args[1];
        Path outputPath = new Path(args[2]);
        // 设置作业的Mapper相关信息
        job.setMapperClass(NovelCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置作业的Reducer相关信息
        job.setReducerClass(NovelCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        // 设置Partitioner为NovelCountPartitioner
        job.setPartitionerClass(NovelCountPartitioner.class);
        // 设置FileInputFormat的输入路径和FileOutputFormat的输出路径
        FileInputFormat.addInputPath(job, novelPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        // 将拼接文件通过DistributedCache机制分发到所有的结点上
        job.addCacheFile(new URI(concatenatedFilePath + "#concatenatedFile"));
        // 提交作业，等待执行完成
        job.waitForCompletion(true);
    }
}
