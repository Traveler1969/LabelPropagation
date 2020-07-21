import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LPAIterator {
    private static boolean finish = false;

    public static void main(String[] args) throws Exception {
        // 创建作业，设置作业的基本信息，并将KeyValueTextInputFormat的分隔符设置为空格，
        // 将TextOutputFormat的分隔符也设置为空格
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = Job.getInstance(conf, "label propagation iteration");
        job.setJarByClass(LPAIterator.class);
        // 从命令行参数中获取输入路径、输出路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        // 设置作业的Mapper相关信息
        job.setMapperClass(LPAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 设置作业的Reducer相关信息
        job.setReducerClass(LPAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        // 设置FileInputFormat的输入路径和FileOutputFormat的输出路径，
        // 并设置InputFormat为KeyValueTextInputFormat
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 提交作业，等待执行完成
        job.waitForCompletion(true);
        // 检查RECORD_COUNTER和UPDATE_COUNTER的计数，判断是否满足迭代终止条件
        Counters counters = job.getCounters();
        long recordCount = counters.findCounter("STATE", "RECORD_COUNTER").getValue();
        long updateCount = counters.findCounter("STATE", "UPDATE_COUNTER").getValue();
        if((double)updateCount / recordCount <= 0.005) {
            finish = true;
        }
    }

    public static boolean termConditionMet() {
        return finish;
    }
}
