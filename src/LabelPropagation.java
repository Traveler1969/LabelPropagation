import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LabelPropagation {
    private static int numIterations = 0;
    /**
     * LabelPropagation.main()需要的命令行参数：args[0]为输入目录，即任务3的输出所在目录
     * args[1]为输出目录，在迭代过程中产生的多个输出目录将作为它的子目录
     * args[2]为原数据所在目录，即/MP_Data/task2/wuxia_novels
     */
    public static void main(String[] args) throws Exception {
        // 记录程序开始时间，用于计算总用时
        long startTime = System.currentTimeMillis();
        // 首先调用InverseListBuilder.main()方法，将任务3的输出转化为初始逆邻接表
        String[] listBuilderInput = {args[0], args[1] + "/iteration_0"};
        InverseListBuilder.main(listBuilderInput);
        // 加载HDFS配置文件并获取HDFS，这是为在迭代过程中删除中间结果做的准备
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        // 调用LPAIterator，执行标签传播算法，迭代直至满足条件
        while(!LPAIterator.termConditionMet()) {
            String[] iteratorInput = {args[1] + "/iteration_" + numIterations,
                    args[1] + "/iteration_" + (numIterations + 1)};
            LPAIterator.main(iteratorInput);
            // 删除上一次迭代的中间结果，节省存储空间
            hdfs.delete(new Path(args[1] + "/iteration_" + numIterations), true);
            numIterations++;
        }
        // 调用GexfBuilder.main()方法，将最终迭代的输出转化为可被Gephi读取的Gexf文件
        String[] gexfBuilderInput = {args[1] + "/iteration_" + numIterations,
                "labelledGraph.gexf"};
        GexfBuilder.main(gexfBuilderInput);
        // 调用LabelViewer.main()方法，将最终迭代的输出中具有相同tag的人名输出到一起
        String[] labelViewerInput = {args[1] + "/iteration_" + numIterations,
                args[1] + "/sortByLabel"};
        LabelViewer.main(labelViewerInput);
        // 调用FileConcatenator.main()方法，将LabelViewer的输出拼接为单个文件
        String[] fileConcatenatorInput = {args[1] + "/sortByLabel",
                args[1] + "/concatenatedFile"};
        FileConcatenator.main(fileConcatenatorInput);
        // 调用LabelNovelCounter.main()方法，获取tag和作品名之间的对应关系
        String[] labelNovelCounterInput = {args[2], args[1] + "/concatenatedFile",
                args[1] + "/labelToNovel"};
        LabelNovelCounter.main(labelNovelCounterInput);
        // 删除临时文件"concatenatedFile"
        hdfs.delete(new Path(args[1] + "/concatenatedFile"), true);
        // 调用NovelViewer.main()方法，将人名按作品名分类输出
        String[] novelViewerInput = {args[1] + "/sortByLabel", args[1] + "/labelToNovel",
                args[1] + "/sortByNovel"};
        NovelViewer.main(novelViewerInput);
        // 删除最终迭代的输出文件，以及中间结果labelToNovel
        hdfs.delete(new Path(args[1] + "/labelToNovel"), true);
        hdfs.delete(new Path(args[1] + "/iteration_" + numIterations), true);
        // 计算总用时，在Driver端的终端输出总用时和迭代次数
        long endTime = System.currentTimeMillis();
        System.out.println("总用时：" + (endTime - startTime) / 1000 + "s");
        System.out.println("迭代次数：" + numIterations);
    }
}
