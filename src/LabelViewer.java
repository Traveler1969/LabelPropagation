import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelViewer {
    public static void main(String[] args) throws Exception {
        // 创建作业，设置作业的基本信息，并将KeyValueTextInputFormat的分隔符设置为空格，
        // 将TextOutputFormat的分隔符也设置为空格
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = Job.getInstance(conf, "label view");
        job.setJarByClass(LabelViewer.class);
        // 从命令行参数中获取输入路径、输出路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        // 设置作业的Mapper相关信息
        job.setMapperClass(LabelViewMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 设置作业的Reducer相关信息，作业不需要Reducer做额外的工作，直接输出Mapper的输出即可
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        // 设置FileInputFormat的输入路径和FileOutputFormat的输出路径，
        // 并设置InputFormat为KeyValueTextInputFormat
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 提交作业，等待执行完成
        job.waitForCompletion(true);
    }
}
