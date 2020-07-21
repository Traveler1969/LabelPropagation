import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ListBuildReducer对于ListBuildMapper的一组输出，比如<戚芳,{狄云,0.333333;戚长发,0.333333}>，
 * 它先为人名“戚芳”分配一个唯一的tag（假设是1），作为标签传播算法中对应结点的初始tag，然后将几个值对组合在一起，
 * 形成逆邻接表：[狄云,0.333333|戚长发,0.333333]，最后输出键值对：<戚芳,1,[狄云,0.333333|戚长发,0.333333]>，
 * 其中第一个逗号之前为键，第一个逗号之后为值
 */

public class ListBuildReducer
        extends Reducer<Text, Text, Text, Text> {
    private final Text outputValueText = new Text();
    private int nextTag;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 将nextTag初始化为当前reducer的ID，以保证各个reducer生成的tag不会重复
        nextTag = context.getTaskAttemptID().getTaskID().getId();
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 输出的value以tag开始
        StringBuilder outputBuilder = new StringBuilder(String.valueOf(nextTag));
        Iterator<Text> it = values.iterator();
        // 加上各个<人名,边权>键值对
        outputBuilder.append(",[").append(it.next());
        while(it.hasNext()) {
            outputBuilder.append('|').append(it.next());
        }
        // 并以']'为结束
        outputBuilder.append(']');
        outputValueText.set(outputBuilder.toString());
        context.write(key, outputValueText);
        // 更新nextTag的值
        nextTag += context.getNumReduceTasks();
    }
}
