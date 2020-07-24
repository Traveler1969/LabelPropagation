import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


/**
 * NovelCountMapper首先读入拼接后按标签分类的人名文件，建立从人名到标签的映射，
 * 再在map()方法中读入原数据集的每一行，对于出现在一行中的人名，输出<Key:"tag,作品名", Value:1>
 */
public class NovelCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final HashMap<String, Integer> nameTagMap = new HashMap<>();
    private String novelName = null;

    private final Text outputText = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        // 每个Mapper实例会处理一个FileSplit，在setup中可以获取小说名
        String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        novelName = fileName.split("\\.txt|\\.TXT")[0];
        if(!novelName.startsWith("金庸")) {
            return;
        }
        // 打开拼接后的按标签分类的人名文件，建立从人名到标签的映射
        if(context.getCacheFiles() != null && context.getCacheFiles().length != 0) {
            File concatenatedFile = new File("./concatenatedFile");
            BufferedReader br = new BufferedReader(new FileReader(concatenatedFile));
            String inputLine = br.readLine();
            while(inputLine != null) {
                String[] inputLineSplits = inputLine.split("\\s");
                int tag = Integer.parseInt(inputLineSplits[0]);
                String name = inputLineSplits[1];
                nameTagMap.put(name, tag);
                inputLine = br.readLine();
            }
            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if(!novelName.startsWith("金庸")) {
            return;
        }
        String[] wordArray = value.toString().split("\\s");
        for(String word : wordArray) {
            Integer tag = nameTagMap.get(word);
            if(tag != null) {
                outputText.set(tag + "," + novelName);
                context.write(outputText, one);
            }
        }
    }
}
