import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LabelViewMapper类从迭代的最终结果中读取一行，并以tag值为Key，以人名为Value进行输出，
 * 比如说，对于一行输入<Key:"狄云", Value:"1,[戚芳,0.33333|戚长发,0.33333|卜垣,0.33333]"，
 * 它输出<Key:1, Value:"狄云">
 */

public class LabelViewMapper
        extends Mapper<Text, Text, IntWritable, Text> {
    private final IntWritable outputIntWritable = new IntWritable();

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // 按第一个逗号切分value，获取tag值
        String tagString = value.toString().split(",", 2)[0];
        int tag = Integer.parseInt(tagString);
        outputIntWritable.set(tag);
        context.write(outputIntWritable, key);
    }
}
