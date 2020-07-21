public class LabelPropagation {
    private static final int numIterations = 20;
    /**
     * LabelPropagation.main()需要的命令行参数：args[0]为输入目录，即任务3的输出所在目录
     * args[1]为输出目录，在迭代过程中产生的多个输出目录将作为它的子目录
     */
    public static void main(String[] args) throws Exception {
        // 首先调用InverseListBuild.main()方法，将任务3的输出转化为初始逆邻接表
        String[] listBuildInput = {args[0], args[1] + "/iteration_0"};
        InverseListBuilder.main(listBuildInput);
        // 调用LPAIterator，执行标签传播算法，以给定次数进行迭代
        for(int i = 0; i <= numIterations - 1; i++) {
            String[] iterationInput = {args[1] + "/iteration_" + i,
                    args[1] + "/iteration_" + (i + 1)};
            LPAIterator.main(iterationInput);
        }
        // 调用GexfBuild.main()方法，将最终迭代的输出转化为可被Gephi读取的Gexf文件
        String[] GexfBuildInput = {args[1] + "/iteration_" + numIterations,
                "labelledGraph.gexf"};
        GexfBuilder.main(GexfBuildInput);
    }
}
