import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 如果value以"金庸"开头，则键值对来自LabelNovelCounter，设置NovelViewBean的novelName，
 * 否则键值对来自LabelViewer，设置NovelViewBean的peopleName，
 * 此外对两者都要设置tag
 */
public class NovelViewMapper
        extends Mapper<Text, Text, NovelViewBean, NullWritable> {
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        NovelViewBean bean = new NovelViewBean();
        String valueString = value.toString();
        if(valueString.startsWith("金庸")) {
            bean.setNovelName(valueString);
        } else {
            bean.setPeopleName(valueString);
        }
        bean.setTag(Integer.parseInt(key.toString()));
        context.write(bean, NullWritable.get());
    }
}
