import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NovelViewer {
    public static void main(String[] args) throws Exception {
        // 创建作业，设置作业的基本信息，并将KeyValueTextInputFormat的分隔符设置为空格，
        // 将TextOutputFormat的分隔符也设置为空格
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = Job.getInstance(conf, "novel view");
        job.setJarByClass(NovelViewer.class);
        // 从命令行参数中获取LabelViewer输入路径、LabelNovelCounter输入路径、输出路径
        Path lvInputPath = new Path(args[0]);
        Path lncInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        // 设置作业的Mapper相关信息
        job.setMapperClass(NovelViewMapper.class);
        job.setMapOutputKeyClass(NovelViewBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置作业的Reducer相关信息
        job.setReducerClass(NovelViewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        // 设置FileInputFormat的输入路径和FileOutputFormat的输出路径，
        // 并设置InputFormat为KeyValueTextInputFormat
        FileInputFormat.addInputPath(job, lvInputPath);
        FileInputFormat.addInputPath(job, lncInputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 提交作业，等待执行完成
        job.waitForCompletion(true);
    }
}
