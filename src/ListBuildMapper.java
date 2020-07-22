import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ListBuildMapper对于任务3的一行输入，比如狄云 [戚芳,0.333333|戚长发,0.333333|卜垣,0.333333]，
 * 将它转化为如下的几个键值对输出：<Key:戚芳,Value:狄云,0.333333>，<Key:戚长发,Value:狄云,0.333333>，
 * <Key:卜垣,Value:狄云,0.333333>，
 */
public class ListBuildMapper
        extends Mapper<Text, Text, Text, Text> {
    private final Text outputKeyText = new Text();
    private final Text outputValueText = new Text();

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // 先将value字符串切分为<人名，边权>的数组
        String[] nameWeightArray = value.toString().split("[\\[|\\]]");
        // 对每对<人名，边权>，以人名为键，"key,边权"为值输出一个键值对
        for(String nameWeightPair : nameWeightArray) {
            if(nameWeightPair.isEmpty()) {
                continue;
            }
            String name = nameWeightPair.split(",")[0];
            String weight = nameWeightPair.split(",")[1];
            outputKeyText.set(name);
            outputValueText.set(key.toString() + "," + weight);
            context.write(outputKeyText, outputValueText);
        }
    }
}
