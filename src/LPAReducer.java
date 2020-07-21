import java.io.IOException;
import java.text.DecimalFormat;

import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LPAReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> graph= new HashMap<>();
        StringBuilder result = new StringBuilder();
        Double weight;
        Integer tag,targettag=0,formerTag=0;
        for (Text val:values)
        {
            String temp=val.toString();
            if(temp.endsWith("]")) {
                // 按第一个逗号切分temp，获得原tag和逆邻接表
                String[] tempSplits = temp.split(",", 2);
                formerTag = Integer.parseInt(tempSplits[0]);
                result.append(tempSplits[1]);
            }
            else
            {
                tag=Integer.parseInt(temp.split(",")[0]);
                weight=Double.parseDouble(temp.split(",")[1]);
                if(graph.containsKey(tag))
                    graph.put(tag,graph.get(tag)+weight);
                else
                    graph.put(tag,weight);
            }
        }

        List<Map.Entry<Integer,Double>> list = new ArrayList(graph.entrySet());
        Collections.sort(list, (o1, o2) -> (o2.getValue().compareTo(o1.getValue())));//按value降序排序
        /*Collections.sort(list,new Comparator<Map.Entry<Integer,Double>>() {
            public int compare(Map.Entry<Integer,Double> o1, Map.Entry<Integer,Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
            */
        Double maxValue = list.get(0).getValue();
        ArrayList<Integer> TagPool = new ArrayList<Integer>();
        for(Map.Entry<Integer,Double> entry:list)
        {
            if(entry.getValue().compareTo(maxValue)==0)
                TagPool.add(entry.getKey());
        }
        int random = (int)(Math.random()*TagPool.size());
        Integer chosenTag = list.get(random).getKey();
        result.insert( 0 , chosenTag.toString()+",");
        context.write(key,new Text(result.toString()));
        // 如果更新后的tag与原tag不同，增加UPDATE_COUNTER的值1
        if(chosenTag.intValue() != formerTag) {
            context.getCounter("STATE", "UPDATE_COUNTER").increment(1);
        }
    }
}
