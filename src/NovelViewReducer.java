import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * NovelViewReducer记录当前tag对应的作品名，并将人名按作品名分类输出
 */
public class NovelViewReducer
        extends Reducer<NovelViewBean, NullWritable, Text, Text> {
    private final Text outputKeyText = new Text();
    private final Text outputValueText = new Text();

    @Override
    protected void reduce(NovelViewBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        if(!key.getNovelName().equals("")) {
            outputKeyText.set(key.getNovelName());
        } else {
            outputValueText.set(key.getPeopleName());
            context.write(outputKeyText, outputValueText);
        }
    }
}
