import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * NovelCountPartitioner对于"tag,作品名"形式的key，仅使用其中的tag来分区
 */
public class NovelCountPartitioner
        extends HashPartitioner<Text, IntWritable> {
    private final Text tagText = new Text();

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String tag = key.toString().split(",")[0];
        tagText.set(tag);
        return super.getPartition(tagText, value, numReduceTasks);
    }
}
