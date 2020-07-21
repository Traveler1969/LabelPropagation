import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 对于逆邻接表中的一项，如<Key:"狄云", Value:"1,[戚芳,0.33333|戚长发,0.33333|卜垣,0.33333]">，
 * Map输出以下两种键值对：第一种是当前结点对邻接结点的tag影响，如<Key:"戚芳", Value:"1,0.33333">，
 * 第二种是逆邻接表本身，用于维持迭代间图的结构，如<Key:"狄云", Value:"[戚芳,0.33333|戚长发,0.33333|卜垣,0.33333]">
 */

public class LPAMapper extends Mapper<Text, Text, Text, Text> {
    private final Text outputKeyText = new Text();
    private final Text outputValueText = new Text();

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // 对每条记录，增加RECORD_COUNTER计数器的值1
        context.getCounter("STATE", "RECORD_COUNTER").increment(1);
        // 先按第一个逗号切分value，取得tag和其后的逆邻接表结构
        String[] valueSplits = value.toString().split(",", 2);
        String tag = valueSplits[0];
        String inverseList = valueSplits[1];
        // 切分逆邻接表，输出当前结点对各个邻接结点的tag影响
        String[] nameWeightArray = inverseList.split("[\\[|\\]]");
        for(String nameWeightPair : nameWeightArray) {
            if(nameWeightPair.isEmpty()) {
                continue;
            }
            outputKeyText.set(nameWeightPair.split(",")[0]);
            outputValueText.set(tag + "," + nameWeightPair.split(",")[1]);
            context.write(outputKeyText, outputValueText);
        }
        // 输出逆邻接表结构
        context.write(key, value);
    }
}
