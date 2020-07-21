import it.uniroma1.dis.wsngroup.gexf4j.core.*;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Calendar;
import java.util.HashMap;

public class GexfBuilder {
    public static void main(String[] args) throws Exception {
        // 加载HDFS配置文件，获取文件系统
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        // 从命令行参数中获取输入路径，即任务5最终迭代的输出路径
        Path inputPath = new Path(args[0]);
        FileStatus[] status = hdfs.listStatus(inputPath);
        // 创建Gexf，设置其基本信息，准备遍历文件
        Gexf gexf = new GexfImpl();
        Calendar date = Calendar.getInstance();
        gexf.getMetadata()
                .setLastModified(date.getTime())
                .setCreator("2020st46")
                .setDescription("标签传播算法的输出结果");
        gexf.setVisualization(true);
        // 设置图的属性
        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC);
        // 设置结点的属性
        AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
        graph.getAttributeLists().add(attrList);
        Attribute attrTag = attrList.createAttribute("0", AttributeType.INTEGER, "tag");
        // 使用Map记录人名到结点之间的映射，以便第二次遍历时将边加入图中
        HashMap<String, Node> nameNodeMap = new HashMap<>();
        // 进行第一次遍历，建立结点
        for(FileStatus curStatus : status) {
            FSDataInputStream in = hdfs.open(curStatus.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String inputLine = br.readLine();
            while(inputLine != null) {
                // 首先按空格分割，获取人名和其后的内容
                String[] inputLineSplits = inputLine.split("\\s");
                String name = inputLineSplits[0];
                // 将后半部分按第一个逗号分割，获取tag
                String tag = inputLineSplits[1].split(",", 2)[0];
                // 创建结点并加入图和映射中
                Node newNode = graph.createNode();
                newNode.setLabel(name).getAttributeValues().addValue(attrTag, tag);
                nameNodeMap.put(name, newNode);
                // 读取下一行
                inputLine = br.readLine();
            }
            br.close();
        }
        // 进行第二次遍历，加入有向边
        for(FileStatus curStatus : status) {
            FSDataInputStream in = hdfs.open(curStatus.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String inputLine = br.readLine();
            while(inputLine != null) {
                // 首先按空格分割，获取人名、当前结点和其后的内容
                String[] inputLineSplits = inputLine.split("\\s");
                String name = inputLineSplits[0];
                Node curNode = nameNodeMap.get(name);
                // 将后半部分按第一个逗号分割，获取逆邻接表
                String inverseList = inputLineSplits[1].split(",", 2)[1];
                // 将逆邻接表按'['、']'和'|'切分，获取各条边的源结点和权重
                String[] nameWeightArray = inverseList.split("[\\[|\\]]");
                for(String nameWeightPair : nameWeightArray) {
                    if(nameWeightPair.isEmpty()) {
                        continue;
                    }
                    Node srcNode = nameNodeMap.get(nameWeightPair.split(",")[0]);
                    double weight = Double.parseDouble(nameWeightPair.split(",")[1]);
                    srcNode.connectTo(curNode).setEdgeType(EdgeType.DIRECTED).setWeight((float)weight);
                }
                inputLine = br.readLine();
            }
            br.close();
        }
        // 将图写入Gexf文件中
        String outputPath = args[1];
        StaxGraphWriter graphWriter = new StaxGraphWriter();
        File outputFile = new File(outputPath);
        Writer outputWriter = new FileWriter(outputFile, false);
        graphWriter.writeToStream(gexf, outputWriter, "UTF-8s");
    }
}
