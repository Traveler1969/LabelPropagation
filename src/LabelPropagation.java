public class LabelPropagation {
    private static int numIterations = 0;
    /**
     * LabelPropagation.main()需要的命令行参数：args[0]为输入目录，即任务3的输出所在目录
     * args[1]为输出目录，在迭代过程中产生的多个输出目录将作为它的子目录
     */
    public static void main(String[] args) throws Exception {
        // 记录程序开始时间，用于计算总用时
        long startTime = System.currentTimeMillis();
        // 首先调用InverseListBuild.main()方法，将任务3的输出转化为初始逆邻接表
        String[] listBuildInput = {args[0], args[1] + "/iteration_0"};
        InverseListBuilder.main(listBuildInput);
        // 调用LPAIterator，执行标签传播算法，迭代直至满足条件
        while(!LPAIterator.termConditionMet()) {
            String[] iterationInput = {args[1] + "/iteration_" + numIterations,
                    args[1] + "/iteration_" + (numIterations + 1)};
            LPAIterator.main(iterationInput);
            numIterations++;
        }
        // 调用GexfBuild.main()方法，将最终迭代的输出转化为可被Gephi读取的Gexf文件
        String[] GexfBuildInput = {args[1] + "/iteration_" + numIterations,
                "labelledGraph.gexf"};
        GexfBuilder.main(GexfBuildInput);
        // 计算总用时，在Driver端的终端输出总用时和迭代次数
        long endTime = System.currentTimeMillis();
        System.out.println("总用时：" + (endTime - startTime) / 1000 + "s");
        System.out.println("迭代次数：" + numIterations);
    }
}
