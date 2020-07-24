import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 对于NovelCountMapper输出的<Key:"tag,作品名", Value:1>形式的键值对，
 * NovelCountReducer将value相加从而得到tag在各个作品中出现的次数，
 * 并且选择其中出现次数最多的作品作为当前tag所属的作品
 */
public class NovelCountReducer
        extends Reducer<Text, IntWritable, IntWritable, Text> {
    // tag相关数据，在tag变更时重置
    private int lastTag = -1;
    private int maxCount = 0;
    private String maxCountNovelName = null;

    private final IntWritable outputIntWritable = new IntWritable();
    private final Text outputText = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] keySplits = key.toString().split(",");
        int tag = Integer.parseInt(keySplits[0]);
        String novelName = keySplits[1];
        // 检查tag是否发生变化
        if(tag != lastTag && lastTag != -1) {
            outputIntWritable.set(lastTag);
            outputText.set(maxCountNovelName);
            context.write(outputIntWritable, outputText);
            maxCount = 0;
        }
        lastTag = tag;
        // 将values中的值相加，与maxCount进行比较
        int currentCount = 0;
        for(IntWritable value : values) {
            currentCount += value.get();
        }
        if(currentCount > maxCount) {
            maxCount = currentCount;
            maxCountNovelName = novelName;
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        if(lastTag != -1) {
            outputIntWritable.set(lastTag);
            outputText.set(maxCountNovelName);
            context.write(outputIntWritable, outputText);
        }
    }
}
