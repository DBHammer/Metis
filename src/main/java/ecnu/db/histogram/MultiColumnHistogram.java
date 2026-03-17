package ecnu.db.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gurobi.gurobi.*;
import ecnu.db.histogram.single.SingleColumnHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static ecnu.db.histogram.RangeType.GREATER_EQ;
import static ecnu.db.histogram.RangeType.LESS_EQ;

public class MultiColumnHistogram {
    private static final Logger logger = LoggerFactory.getLogger(MultiColumnHistogram.class);
    private static final double SCALE_FACTOR = 1e3;

    /**
     * 记录每个列之前的累计维度的乘积，不包含该列。第一列设置为1。不在直方图上的列设置为0。
     */
    private final int[] cumulativeDimension;
    /**
     * 记录该列的直方图的维度数量。不在直方图上的列设置为1。
     */
    private final int[] columnDimension;
    /**
     * columnNames是[parentColumns, newColumns]的集合
     */
    private final List<String> columnNames;
    private GRBEnv env = null;
    private GRBModel model = null;
    private GRBVar[] variables;

    /**
     * 从这个index开始，后续的column为新加的列
     */
    private final int firstAddedColumnIndex;
    /**
     * 多维直方图中，每个多维桶对应的概率
     */
    private ConcurrentHashMap<Integer, Double> multiColumnCumulativeIndex2Probability;
    /**
     * 多维直方图存储文件的物理大小
     */
    private double MDHistSizeInKB;


    public MultiColumnHistogram(List<String> columnNames, int[] columnDimension, int firstAddedColumnIndex) {
        this.columnNames = columnNames;
        this.columnDimension = columnDimension;

        this.cumulativeDimension = new int[columnDimension.length];
        int currentCumulativeDimension = 1;
        for (int i = columnDimension.length - 1; i >= 0; i--) {
            cumulativeDimension[i] = currentCumulativeDimension;
            currentCumulativeDimension *= columnDimension[i];
        }

        this.firstAddedColumnIndex = firstAddedColumnIndex;
        this.multiColumnCumulativeIndex2Probability = new ConcurrentHashMap<>();

        try {
            // 初始化 Gurobi 环境
            env = new GRBEnv(true);

            if (logger.isDebugEnabled()) {
                env.set(GRB.IntParam.LogToConsole, 1);
            }else {
                env.set(GRB.IntParam.LogToConsole, 0);
            }

            env.start();

            // 创建模型
            model = new GRBModel(env);

            // 创建变量
            variables = new GRBVar[currentCumulativeDimension];
            for (int i = 0; i < currentCumulativeDimension; i++) {
                variables[i] = model.addVar(
                        0.0,
                        SCALE_FACTOR,
                        0.0,
                        GRB.CONTINUOUS,
                        "v_" + i
                );
            }

            // 约束：所有变量之和 = SCALE_FACTOR
            GRBLinExpr sumExpr = new GRBLinExpr();
            for (GRBVar v : variables) {
                sumExpr.addTerm(1.0, v);
            }
            model.addConstr(sumExpr, GRB.EQUAL, SCALE_FACTOR, "sum_prob");

            logger.info("LP Vars are created, the num of Vars is {}", variables.length);

        } catch (GRBException e) {
            throw new RuntimeException(e);
        }
    }

    // This constructor is for loading an existing MD-Hist, no LP solver required.
    public MultiColumnHistogram(
            List<String> columnNames,
            int[] columnDimension,
            int[] cumulativeDimension,
            int totalDimension,
            ConcurrentHashMap<Integer, Double> multiColumnCumulativeIndex2Probability) {

        this.columnNames = columnNames;
        this.columnDimension = columnDimension;
        this.cumulativeDimension = cumulativeDimension;

        // 复制已存在的 MD-Hist 概率表
        this.multiColumnCumulativeIndex2Probability =
                new ConcurrentHashMap<>(multiColumnCumulativeIndex2Probability);

        // 注意：这是“只读加载模式”，不需要创建 Gurobi 模型
        this.env = null;
        this.model = null;
        this.variables = null;

        // 表示没有新的列被添加
        this.firstAddedColumnIndex = -1;
    }

    /**
     * 计算在当前request中，要建的直方图（[interval, new]）联合直方图中每个格子的覆盖率，然后设置约束
     */
    private void setModelConstraint(Map<String, HistogramRequest> requests, GRBLinExpr expr) {
        if (!new HashSet<>(columnNames).containsAll(requests.keySet())) {
            throw new IllegalStateException("多列直方图不包含请求的 request 列");
        }

        var columnIndex2Request = transformRequest2Index(requests);

        IntStream.range(0, variables.length)
                .parallel()
                .forEach(rowId -> {
                    double ratio = computeRatio(rowId, columnIndex2Request);
                    if (ratio > 0) {
                        synchronized (expr) {
                            expr.addTerm(ratio, variables[rowId]);
                        }
                    }
                });
    }

    /**
     * 将请求的列名转换为该列在columnNames中的索引
     */
    private Map<Integer, HistogramRequest> transformRequest2Index(Map<String, HistogramRequest> requests) {
        Map<Integer, HistogramRequest> columnIndex2Request = HashMap.newHashMap(requests.size());
        for (var columnName2Request : requests.entrySet()) {
            columnIndex2Request.put(columnNames.indexOf(columnName2Request.getKey()), columnName2Request.getValue());
        }
        return columnIndex2Request;
    }

    /**
     * rowId是多列直方图的一个格子的索引，计算该格子的概率
     *
     * @param rowId             多列直方图的一个格子的索引
     * @param requests          请求的列名到请求的映射
     * @return 多列直方图的一个格子的概率
     */
    private double computeRatio(int rowId, Map<Integer, HistogramRequest> requests) {
        double multipleHistogramRatio = 1;
        for (var columnIndex2Request : requests.entrySet()) {
            int columnIndex = columnIndex2Request.getKey();
            int index = (rowId / cumulativeDimension[columnIndex]) % columnDimension[columnIndex];
            HistogramRequest request = columnIndex2Request.getValue();
            if (index == request.rangeIndex()) {
                multipleHistogramRatio *= request.currentRangeRatio();
            } else {
                boolean satisfyLess = request.rangeType() == LESS_EQ && index < request.rangeIndex();
                boolean satisfyGreaterCond = index > request.rangeIndex() && index < columnDimension[columnIndex] - 1;
                boolean satisfyGreater = request.rangeType() == GREATER_EQ && satisfyGreaterCond;
                if (!satisfyLess && !satisfyGreater) {
                    return 0;
                }
            }
        }
        return multipleHistogramRatio;
    }

    /**
     * 设置约束，使得直方图的新增列分布与单列直方图的分布一致
     *
     * @param columnName 列名
     * @param histogram  单列直方图
     */
    public void setConstraintFromAddedSingleColumnHistogram(String columnName,
                                                            SingleColumnHistogram<?> histogram) {
        var iterator = histogram.iterator();

        while (iterator.hasNext()) {
            var requestAndProbability = iterator.next();
            double probability = requestAndProbability.getValue();

            // 1. 创建线性表达式
            GRBLinExpr expr = new GRBLinExpr();

            // 2. 加入变量项
            setModelConstraint(
                    Collections.singletonMap(columnName, requestAndProbability.getKey()),
                    expr
            );

            try {
                // 3. 添加等式约束 expr == probability
                model.addConstr(expr, GRB.EQUAL, probability * SCALE_FACTOR,
                        "singlecol_" + columnName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 设置来自query的精确、等值约束
     */
    public void setAccurateEqualConstraint(Map<String, HistogramRequest> column2Requests,
                                           double queryProbability) {

        try {
            // Slack 变量
            GRBVar toUpError = model.addVar(
                    0, (1 - queryProbability) * SCALE_FACTOR,
                    0.0, GRB.CONTINUOUS, "upErr"
            );

            GRBVar toDownError = model.addVar(
                    0, queryProbability * SCALE_FACTOR,
                    0.0, GRB.CONTINUOUS, "downErr"
            );

            // 构造表达式 expr
            GRBLinExpr expr = new GRBLinExpr();

            expr.addTerm(-1.0, toUpError);
            expr.addTerm( 1.0, toDownError);

            // 加入 MD-Hist 变量项
            setModelConstraint(column2Requests, expr);

            // 添加 expr == queryProbability 约束
            model.addConstr(expr,
                    GRB.EQUAL,
                    queryProbability * SCALE_FACTOR,
                    "eqQuery"
            );

            // 目标函数：加入 slack 惩罚
            toUpError.set(GRB.DoubleAttr.Obj, SCALE_FACTOR);
            toDownError.set(GRB.DoubleAttr.Obj, SCALE_FACTOR);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 设置来自query的精确，大于等于约束
     * @param column2Requests
     * @param maxLowerBoundProbability
     */
    public void setAccurateGreaterEqualConstraint(
            Map<String, HistogramRequest> column2Requests,
            double maxLowerBoundProbability) {

        try {
            GRBVar toDownError = model.addVar(
                    0,
                    maxLowerBoundProbability * SCALE_FACTOR,
                    0.0,
                    GRB.CONTINUOUS,
                    "downErr_ge"
            );

            GRBLinExpr expr = new GRBLinExpr();
            expr.addTerm(1.0, toDownError);

            setModelConstraint(column2Requests, expr);

            // 下界：expr >= maxLowerBoundProbability
            model.addConstr(
                    expr,
                    GRB.GREATER_EQUAL,
                    maxLowerBoundProbability * SCALE_FACTOR,
                    "geQuery_lb"
            );

            // 上界：expr <= 1
            model.addConstr(
                    expr,
                    GRB.LESS_EQUAL,
                    1.0 * SCALE_FACTOR,
                    "geQuery_ub"
            );

            // 目标函数惩罚 slack
            toDownError.set(GRB.DoubleAttr.Obj, SCALE_FACTOR);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 求解LP问题
     */
    public void solveDistribution() {
        try {
            model.optimize();

            int status = model.get(GRB.IntAttr.Status);
            if (status != GRB.OPTIMAL) {
                throw new IllegalStateException("Gurobi cannot find optimal solution. Status = " + status);
            }

            // 保存非零概率
            for (int i = 0; i < variables.length; i++) {
                double val = variables[i].get(GRB.DoubleAttr.X);
                if (val > 0) {
                    multiColumnCumulativeIndex2Probability.put(i, val / SCALE_FACTOR);
                }
            }

            double sum = multiColumnCumulativeIndex2Probability.values()
                    .stream().mapToDouble(Double::doubleValue).sum();

            logger.info("Sum of all probabilities: {}", sum);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取子直方图的累计维度
     *
     * @return 子直方图的累计维度
     */
    private int getChildCumulativeDimension() {
        if (firstAddedColumnIndex > 0) {
            return cumulativeDimension[firstAddedColumnIndex - 1];
        } else {
            return cumulativeDimension[0] * columnDimension[0];
        }
    }

    /**
     * 获取多维直方图的列名
     * @return
     */
    public List<String> getColumnNames(){
        return this.columnNames;
    }

    /**
     * 获取所有组合项的最终加权概率值：
     * 每个组合项的累计索引查 multiColumnCumulativeIndex2Probability，如果存在则乘以 ratio，否则为 0
     * @param perDimBuckets 每个维度的桶集合
     * @return 所有组合项对应的最终概率值数组
     */
    public double getProbabilitiesFromCumulativeIndexWithRatio(
            List<List<BucketOfSingleColumn>> perDimBuckets) {

        if (perDimBuckets == null || perDimBuckets.isEmpty()) {
            return 0.0;
        }

        // 关键：任何一维为空 → 概率为 0
        for (List<BucketOfSingleColumn> dimBuckets : perDimBuckets) {
            if (dimBuckets == null || dimBuckets.isEmpty()) {
                return 0.0;
            }
        }

        int dims = perDimBuckets.size();
        if (dims == 0) return 0.0;

        int[][] indicesPerDim = new int[dims][];
        double[][] ratiosPerDim = new double[dims][];
        int[] sizes = new int[dims];

        for (int i = 0; i < dims; i++) {
            List<BucketOfSingleColumn> dimBuckets = perDimBuckets.get(i);
            sizes[i] = dimBuckets.size();
            indicesPerDim[i] = new int[sizes[i]];
            ratiosPerDim[i] = new double[sizes[i]];
            for (int j = 0; j < sizes[i]; j++) {
                BucketOfSingleColumn bucket = dimBuckets.get(j);
                indicesPerDim[i][j] = bucket.index();
                ratiosPerDim[i][j] = bucket.ratio();
            }
        }

        // 转换Map为数组
        int maxIndex = calculateMaxCumulativeIndex(indicesPerDim);
        double[] probabilityArray = convertMapToArray(multiColumnCumulativeIndex2Probability, maxIndex);

        // 累加
        int[] idx = new int[dims];
        double sum = 0.0;

        while (true) {
            int cumulativeIndex = 0;
            double combinedRatio = 1.0;

            for (int i = 0; i < dims; i++) {
                cumulativeIndex += indicesPerDim[i][idx[i]] * cumulativeDimension[i];
                combinedRatio *= ratiosPerDim[i][idx[i]];
            }

            double prob = (cumulativeIndex >= 0 && cumulativeIndex < probabilityArray.length)
                    ? probabilityArray[cumulativeIndex] : 0.0;
            sum += prob * combinedRatio;

            // 进位逻辑
            int d = dims - 1;
            while (d >= 0) {
                idx[d]++;
                if (idx[d] < sizes[d]) break;
                idx[d] = 0;
                d--;
            }
            if (d < 0) break;
        }

        return sum;
    }

    /**
     * 计算最大可能的累积索引
     */
    private int calculateMaxCumulativeIndex(int[][] indicesPerDim) {
        int maxIndex = 0;
        for (int i = 0; i < indicesPerDim.length; i++) {
            int maxInDim = 0;
            for (int index : indicesPerDim[i]) {
                if (index > maxInDim) maxInDim = index;
            }
            maxIndex += maxInDim * cumulativeDimension[i];
        }
        return maxIndex + 1;
    }

    /**
     * 将ConcurrentHashMap转换为数组
     */
    private double[] convertMapToArray(ConcurrentHashMap<Integer, Double> map, int size) {
        double[] array = new double[size];
        Arrays.fill(array, 0.0);
        // 填充已有值
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            int key = entry.getKey();
            if (key >= 0 && key < size) {
                array[key] = entry.getValue();
            }
        }
        return array;
    }

    /**
     * 获取所有组合项的最终概率值：
     * 每个组合项的累计索引查 multiColumnCumulativeIndex2Probability，如果存在则返回对应的概率值，否则为 0
     * @param perDimIndex 每个维度的索引集合
     * @return 所有组合项对应的最终概率值数组
     */
    public double[] getProbabilitiesFromCumulativeIndex(List<List<Integer>> perDimIndex) {
        int dims = perDimIndex.size();
        // 计算所有组合项的总数
        int totalSize = perDimIndex.stream().mapToInt(List::size).reduce(1, (a, b) -> a * b);
        double[] result = new double[totalSize];

        int[] idx = new int[dims];
        int[] sizes = perDimIndex.stream().mapToInt(List::size).toArray();

        int pos = 0;
        while (true) {
            int cumulativeIndex = 0;

            // 计算当前组合项的累计索引
            for (int i = 0; i < dims; i++) {
                cumulativeIndex += perDimIndex.get(i).get(idx[i]) * cumulativeDimension[i]; // cumulativeDimension 需要根据你具体的实现
            }

            // 从 multiColumnCumulativeIndex2Probability 中获取概率值
            double prob = multiColumnCumulativeIndex2Probability.getOrDefault(cumulativeIndex, 0.0);
            result[pos++] = prob;

            // 进位逻辑，模拟多维数组的遍历
            int d = dims - 1;
            while (d >= 0) {
                idx[d]++;
                if (idx[d] < sizes[d]) break;
                idx[d] = 0;
                d--;
            }
            if (d < 0) break;
        }

        return result;
    }

    /**
     * 将本直方图导出为 JSON 文件，同时记录理论存储大小（单位：KB）
     *  理论大小定义为：桶数量 × 8 字节 / 1024
     * @param outputDir 输出目录（例如 "./output"），不需要以 "/" 结尾
     * @param mapper    ObjectMapper 实例（由调用者统一传入）
     * @return 保存后的文件大小（字节），失败返回 0
     */
    public void exportAndMeasureSize(String outputDir, ObjectMapper mapper) {
        Map<String, Object> info = new HashMap<>();
        info.put("columnNames", columnNames);
        info.put("columnDimension", columnDimension);
        info.put("cumulativeIndex2Probability", multiColumnCumulativeIndex2Probability);

        String filename = "multi_column_" + String.join("_", columnNames) + ".json";
        String filePath = outputDir + "/" + filename;

        // 计算理论物理存储大小（每个桶一个 double，8 字节）
        int nonEmptyBucketNum = multiColumnCumulativeIndex2Probability.size();
        this.MDHistSizeInKB = nonEmptyBucketNum * (4.0 + 8.0) / 1024.0;

        try (FileWriter writer = new FileWriter(filePath)) {
            mapper.writeValue(writer, info);
            long exportedSizeInBytes = new File(filePath).length();
            double exportedSizeInKB = exportedSizeInBytes / 1024.0;

            logger.info(
                    "Multi-column [{}]: logical size = {} KB ({} buckets), exported file size = {} KB",
                    String.join(", ", this.getColumnNames()),
                    String.format("%.2f", this.MDHistSizeInKB),
                    nonEmptyBucketNum,
                    String.format("%.2f", exportedSizeInKB)
            );
        } catch (IOException e) {
            logger.error("Failed to export histogram to file: {}", filePath, e);
        }
    }

    /**
     * 返回当前多列直方图的物理存储大小（单位：KB，保留两位小数）
     * @return 直方图保存后的大小（以KB为单位）
     */
    public double getMDHistSizeInKB() {
        return this.MDHistSizeInKB;
    }


}
