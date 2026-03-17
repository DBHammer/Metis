package ecnu.db.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import ecnu.db.histogram.adaptor.ColumnTypeAdaptor;
import ecnu.db.histogram.single.SingleColumnHistogram;
import ecnu.db.queylog.single.ColumnQueryRequest;
import ecnu.db.queylog.single.SingleTableQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.gurobi.gurobi.*;

public class HistogramManager {
    private static final Logger logger = LoggerFactory.getLogger(HistogramManager.class);

    private static final String Q_ERROR_RESULT_FILE = "./output/qErrorResult/";
    private static final String MD_HIST_FILE = "./output/";
    private static int tableSize;

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 所有列 -> 直方图
     */
    private final Map<String, SingleColumnHistogram<?>> columnName2Histogram;
    /**
     * 所有已建立的直方图
     */
    private final List<MultiColumnHistogram> multiColumnHistograms = new ArrayList<>();
    /**
     * 多维直方图池子，用于施加父直方图约束时复用，无需最终存储
     */
    private final List<MultiColumnHistogram> poolOfMultiColumnHistograms = new ArrayList<>();
    /**
     * 存储已构建过的列，用于识别parentColumn和addedColunm
     */
    private Set<String> constructedColumnNames = new HashSet<>();
    /**
     * 已经构建的列 -> 每一行的range索引
     * int[]是表大小，记录每一列上每一行对应的该列的单列直方图的range索引
     */
    private final Map<String, int[]> constructedColumnName2RangeIndex = new HashMap<>();
    /**
     * 查询负载
     */
    private final List<SingleTableQueryRequest> singleTableQueryRequests;
    /**
     * 在本次构建中新引入的列
     */
    private final List<String> addedColumn = new ArrayList<>();
    /**
     * 在本次构建中的列
     */
    private final List<String> hisColumn = new ArrayList<>();
    /**
     * 训练阶段，缓存上一次计算的QError，便于复用
     */
    private final double[] cachedQError;
    /**
     * 训练阶段，缓存上一次计算的边权重，便于复用
     */
    private final double[] cachedEdgeWeight;
    /**
     * 缓存全域桶
     */
    private final Map<String, List<BucketOfSingleColumn>> globalDomainBucketCache;
    /**
     * 使用组合数，根据查询涉及的列数对QError进行惩罚
     */
    //    Map<Integer, Integer> combineMap = Map.of(1, 1, 2, 1, 3, 1, 4, 2, 5, 3);
    Map<Integer, Integer> combineMap = Map.of(1, 1, 2, 1, 3, 3, 4, 6, 5, 10);

    /**
     * debug起到约束作用的查询
     */
    private Set<Integer> equalityConstraintQueries = new HashSet<>();
    private Set<Integer> lowerBoundConstraintQueries = new HashSet<>();
    /**
     * 已构建的多维直方图的总物理大小
     */
    public double MDHistTotalSizeInKB = 0.0;
    /**
     * 单列直方图总的物理大小
     */
    public double SingleHistTotalSizeInKB = 0.0;

    /**
     * 计算训练集的结果，返回用于施加CoG边权重的w_ij(即，edgeWeights)，和用于判断是否早停的训练集Q-error(即，qErrors)
     */
    public static class TrainQErrorResult {
        public final double[] qErrors;
        public final double[] edgeWeights;

        public TrainQErrorResult(double[] qErrors, double[] edgeWeights) {
            this.qErrors = qErrors;
            this.edgeWeights = edgeWeights;
        }
    }

    /**
     * 多列直方图管理器
     *
     * @param columnName2Histogram     所有列 -> 直方图
     * @param tableSize                表的大小
     * @param singleTableQueryRequests 查询负载
     */
    public HistogramManager(Map<String, SingleColumnHistogram<?>> columnName2Histogram, int tableSize,
                            List<SingleTableQueryRequest> singleTableQueryRequests, int sampleSize) {
        File mdHistDir = new File(MD_HIST_FILE);
        if (!mdHistDir.exists()) {
            mdHistDir.mkdirs();
        }

        this.tableSize = tableSize;
        this.columnName2Histogram = columnName2Histogram;
        this.singleTableQueryRequests = singleTableQueryRequests;
        this.cachedQError = new double[sampleSize];
        this.cachedEdgeWeight = new double[sampleSize];
        this.globalDomainBucketCache = initGlobalDomainBuckets();
        exportSingleHistogramStorageInfoToFiles();
    }

    /**
     * 预构建全局 WholeDomain Bucket 缓存（线程安全）
     */
    private Map<String, List<BucketOfSingleColumn>> initGlobalDomainBuckets() {
        Map<String, List<BucketOfSingleColumn>> map = new HashMap<>();
        for (String col : columnName2Histogram.keySet()) {
            map.put(col, columnName2Histogram.get(col).getSingleColumnBucketsOfWholeDomain());
        }
        return map;
    }

    /**
     * 根据父直方图和单列直方图初始化多列直方图求解器
     *
     * @param parentColumn 依赖的父直方图
     * @return 多列直方图求解器
     */
    private MultiColumnHistogram makeMultiColumnHistogram(List<String> parentColumn) {
        // 按照参照列和新引入的列的顺序排列
        List<String> orderedHistogramColumn = new ArrayList<>(parentColumn);
        orderedHistogramColumn.addAll(addedColumn);
        logger.debug("ordered histogram column: {}", orderedHistogramColumn);
        // 记录每一个列的直方图大小
        int[] columnSize = new int[orderedHistogramColumn.size()];
        for (int i = 0; i < orderedHistogramColumn.size(); i++) {
            var histogram = columnName2Histogram.get(orderedHistogramColumn.get(i));
            columnSize[i] = histogram.getHistogramSize();
        }
        // 实例化一个多维直方图
        var multiColumnHistogram = new MultiColumnHistogram(orderedHistogramColumn, columnSize, parentColumn.size());
        // 对所有属性列施加单列分布约束
        for (String columnName : orderedHistogramColumn) {
            var histogram = columnName2Histogram.get(columnName);
            multiColumnHistogram.setConstraintFromAddedSingleColumnHistogram(columnName, histogram);
        }
        return multiColumnHistogram;
    }


    public void constructHistogram(Collection<String> histogramColumnNames) {
        logger.info("construct histogram for {}", histogramColumnNames);

        equalityConstraintQueries.clear();
        lowerBoundConstraintQueries.clear();

        List<String> parentColumn = histogramColumnNames.stream().filter(constructedColumnNames::contains).collect(Collectors.toList());
        addedColumn.clear();
        addedColumn.addAll(histogramColumnNames);
        addedColumn.removeAll(parentColumn);

        hisColumn.clear();
        hisColumn.addAll(histogramColumnNames);
        logger.debug("parent columns is {}, added columns is {}", parentColumn, addedColumn);

        MultiColumnHistogram multiColumnHistogram = makeMultiColumnHistogram(parentColumn);

        // Accurate, Equal 约束中被精确刻画的区域（改为用 String key 表示）
        Set<String> accurateEqualCoveredRegions = new HashSet<>();

        // Accurate, GreaterEq 约束中被精确刻画的区域
        Map<String, List<SingleTableQueryRequest>> clusteredFilteredQueries = new HashMap<>();
        // 同一个 key 对应的 ColumnQueryRequest 列表（queryRange）
        Map<String, List<ColumnQueryRequest>> clusteredRanges = new HashMap<>();

        // 首先, 构建精确、等值约束
        for (int queryIndex = 0; queryIndex < singleTableQueryRequests.size(); queryIndex++){
             SingleTableQueryRequest singleTableQueryRequest = singleTableQueryRequests.get(queryIndex);

             if(!singleTableQueryRequest.containAnyColumn(histogramColumnNames)){
                 // 如果当前query不包含任何MD-Hist中的列，直接过滤
                 continue;
             } else if (singleTableQueryRequest.containAnyUnexpectedColumn(histogramColumnNames)) {
                 // 如果当前query包含MD-Hist中的列，但是涉及到不在当前要建的MD-Hist中的列，则构建大于等于约束
                 // 1. 提取当前查询在 histogramColumnNames 上的 ColumnQueryRequest
                 // 注意：singleTableQueryRequest.requests() 在构建时已经按列名排序（见 SingleTableQueryLog），
                 // filter 操作会保持原始顺序，所以 queryRange 的顺序是确定的
                 List<ColumnQueryRequest> queryRange = singleTableQueryRequest.requests().stream()
                         .filter(req -> histogramColumnNames.contains(req.columnName())) // 只保留在 histogramColumnNames 中的谓词
                         .toList();
                 if (queryRange.size() >= 2) {
                     // 2. 为该 queryRange 生成稳定的字符串 key
                     String key = queryRange.stream().map(req -> req.toString()).collect(Collectors.joining("|"));
                     // 3. 将当前查询加入该 cluster
                     clusteredFilteredQueries
                             .computeIfAbsent(key, k -> new ArrayList<>())
                             .add(singleTableQueryRequest);

                     // 4. 记录一次 queryRange（相同 key 的 queryRange 语义一致）
                     clusteredRanges.putIfAbsent(key, queryRange);
                 }
                 continue;
             }
            // 走到这里说明：query 中只包含 histogramColumnNames 内的列
            // 记录该区域已被等值约束精确刻画（用同样的编码方式记录 key）
            List<ColumnQueryRequest> equalRange = singleTableQueryRequest.requests().stream()
                    .filter(req -> histogramColumnNames.contains(req.columnName()))
                    .toList();

            String equalKey = equalRange.stream().map(req -> req.toString()).collect(Collectors.joining("|"));
            accurateEqualCoveredRegions.add(equalKey);

             // 当前query中包含added列，且query中所有列都在当前要建的multi-histogram中，则构建等值约束
             Map<String, HistogramRequest> histogramRequestMap = splitIntoHistogramRequest(singleTableQueryRequest.requests(), histogramColumnNames);
             multiColumnHistogram.setAccurateEqualConstraint(histogramRequestMap, (double) singleTableQueryRequest.cardinality() / tableSize);
             // 设置了等值约束的查询
             equalityConstraintQueries.add(singleTableQueryRequest.query_no());

        }
        logger.info("LP equal constraints created!");

        // =====================
        // 2. 过滤掉已被等值约束覆盖的区域
        //    （key 表示的区域若已在 accurateEqualCoveredRegions 中，则直接跳过）
        // =====================
        clusteredFilteredQueries.entrySet().removeIf(entry ->
                accurateEqualCoveredRegions.contains(entry.getKey())
        );

        // =====================
        // 3. 构建大于等于约束（下界约束）
        // =====================
        clusteredFilteredQueries.entrySet().stream().forEach(entry -> {
            String key = entry.getKey();
            List<SingleTableQueryRequest> cluster = entry.getValue();
            List<ColumnQueryRequest> queryRange = clusteredRanges.get(key);

            // --------------------------
            // 1. 贪心 independent set（确定性）
            // --------------------------
            List<SingleTableQueryRequest> greedySet =
                    findMaxIndependentQueries(cluster, queryRange);
            double greedySum = greedySet.stream()
                    .mapToDouble(SingleTableQueryRequest::cardinality)
                    .sum();

            // --------------------------
            // 2. 用 greedy 结果构建真正的 lower-bound
            // --------------------------
            lowerBoundConstraintQueries.addAll(
                    greedySet.stream()
                            .map(SingleTableQueryRequest::query_no)
                            .collect(Collectors.toSet())
            );

            double maxLowerBound = greedySum / tableSize;

            // 为该区域添加大于等于约束
            Map<String, HistogramRequest> histogramRequestMap =
                    splitIntoHistogramRequest(queryRange, histogramColumnNames);

            synchronized (multiColumnHistogram) { // 确保多线程安全
                multiColumnHistogram.setAccurateGreaterEqualConstraint(histogramRequestMap, maxLowerBound);
            }
        });
        logger.info("LP lower bound constraints created!");

        multiColumnHistogram.solveDistribution();
        logger.info("solve complete！");

        // 存储MD，并计算大小
        multiColumnHistogram.exportAndMeasureSize(MD_HIST_FILE, mapper);
        MDHistTotalSizeInKB += multiColumnHistogram.getMDHistSizeInKB();

        // 保存当前构建的多维直方图
        multiColumnHistograms.add(multiColumnHistogram);

        // 将当前构建的列存入“已构建过的列”中
        constructedColumnNames.addAll(histogramColumnNames);
    }

    /*
    使用ILP求解下限约束
     */
    private List<SingleTableQueryRequest> solveMWISWithGurobi(
            List<SingleTableQueryRequest> cluster,
            Map<SingleTableQueryRequest, Set<SingleTableQueryRequest>> conflictGraph
    ) {
        try {
            GRBEnv env = new GRBEnv(true);
            env.set(GRB.IntParam.LogToConsole, 0);
            env.start();

            GRBModel model = new GRBModel(env);

            int n = cluster.size();

            // 建立变量 x_i
            GRBVar[] vars = new GRBVar[n];
            for (int i = 0; i < n; i++) {
                vars[i] = model.addVar(0, 1, 0, GRB.BINARY, "x_" + i);
            }

            // 添加冲突约束： x_i + x_j <= 1
            for (int i = 0; i < n; i++) {
                SingleTableQueryRequest qi = cluster.get(i);

                if (!conflictGraph.containsKey(qi)) continue;

                for (SingleTableQueryRequest qj : conflictGraph.get(qi)) {
                    int j = cluster.indexOf(qj);
                    if (j > i) {
                        // 避免重复添加约束
                        GRBLinExpr expr = new GRBLinExpr();
                        expr.addTerm(1.0, vars[i]);
                        expr.addTerm(1.0, vars[j]);
                        model.addConstr(expr, GRB.LESS_EQUAL, 1.0, "conf_" + i + "_" + j);
                    }
                }
            }

            // 目标函数：最大化 sum(cardinality_i * x_i)
            GRBLinExpr objective = new GRBLinExpr();
            for (int i = 0; i < n; i++) {
                objective.addTerm(cluster.get(i).cardinality(), vars[i]);
            }
            model.setObjective(objective, GRB.MAXIMIZE);

            // 求解
            model.optimize();

            // 读取解
            List<SingleTableQueryRequest> result = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (vars[i].get(GRB.DoubleAttr.X) > 0.5) {
                    result.add(cluster.get(i));
                }
            }

            model.dispose();
            env.dispose();

            return result;

        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }


    /**
     * 分离出当前query在histogramColumn上的请求
     * @param requests
     * @param histogramColumn
     * @return
     */
    private Map<String, HistogramRequest> splitIntoHistogramRequest(List<ColumnQueryRequest> requests, Collection<String> histogramColumn) {
        Map<String, HistogramRequest> histogramRequestMap = new HashMap<>();
        for (ColumnQueryRequest request : requests) {
            // 抽取出直方图请求
            if (histogramColumn.contains(request.columnName())) {
                var histogram = columnName2Histogram.get(request.columnName());
                var histogramRequest = histogram.getHistogramRequest(request.value(), request.rangeType());
                histogramRequestMap.put(request.columnName(), histogramRequest);
            }
        }
        return histogramRequestMap;
    }

    /**
     * 获得单个cluster中，互不冲突的，基数和最大的查询集合。
     * @param cluster
     * @param queryRange
     * @return
     */
    private List<SingleTableQueryRequest> findMaxIndependentQueries(List<SingleTableQueryRequest> cluster, List<ColumnQueryRequest> queryRange) {
        // 1. 构建冲突图
        Map<SingleTableQueryRequest, Set<SingleTableQueryRequest>> conflictGraph = buildConflictGraph(cluster, queryRange);

        // 2. 贪心选择最大基数和的查询集合
        List<SingleTableQueryRequest> selectedQueries = new ArrayList<>();
        // 使用 LinkedHashSet 保持顺序，或者使用 TreeSet 进行排序以确保确定性
        // 为了确保确定性，我们使用 LinkedHashSet 保持 cluster 的原始顺序
//        List<SingleTableQueryRequest> remainingQueries = cluster;
        Set<SingleTableQueryRequest> remaining = new HashSet<>(cluster);

        while (!remaining.isEmpty()) {
            SingleTableQueryRequest bestQuery = null;
            int maxContribution = Integer.MIN_VALUE;
            int bestQueryNo = Integer.MAX_VALUE; // 当 cardinality 相同时，选择 query_no 最小的

            for (SingleTableQueryRequest query : remaining) {
                // int conflictCardinalitySum = conflictGraph.getOrDefault(query, new HashSet<>()).stream()
                //         .mapToInt(SingleTableQueryRequest::cardinality)
                //         .sum();
                // int contribution = query.cardinality() - conflictCardinalitySum;
                int contribution = query.cardinality();

                // 如果 cardinality 更大，或者 cardinality 相同但 query_no 更小，则更新 bestQuery
                if (contribution > maxContribution || 
                    (contribution == maxContribution && query.query_no() < bestQueryNo)) {
                    maxContribution = contribution;
                    bestQueryNo = query.query_no();
                    bestQuery = query;
                }
            }

            if (bestQuery == null) {
                break;
            }

            // 选择贡献最大的查询
            selectedQueries.add(bestQuery);
            remaining.remove(bestQuery);
            // 移除所有与其冲突的查询
            remaining.removeAll(conflictGraph.getOrDefault(bestQuery, new HashSet<>()));
        }

        return selectedQueries;
    }

    /**
     * 为单个cluster中的查询构建冲突图
     * @param cluster
     * @param queryRange
     * @return
     */
    private Map<SingleTableQueryRequest, Set<SingleTableQueryRequest>> buildConflictGraph(
            List<SingleTableQueryRequest> cluster, List<ColumnQueryRequest> queryRange) {

        Map<SingleTableQueryRequest, Set<SingleTableQueryRequest>> conflictGraph = new HashMap<>();

        for (int i = 0; i < cluster.size(); i++) {
            for (int j = i + 1; j < cluster.size(); j++) {
                SingleTableQueryRequest q1 = cluster.get(i);
                SingleTableQueryRequest q2 = cluster.get(j);

                // 仅检查 queryRange 之外的谓词是否冲突
                if (isQueryConflicting(q1, q2, queryRange)) {
                    conflictGraph.computeIfAbsent(q1, k -> new HashSet<>()).add(q2);
                    conflictGraph.computeIfAbsent(q2, k -> new HashSet<>()).add(q1);
                }
            }
        }

        return conflictGraph;
    }

    /**
     * 判断两个query是否冲突
     * @param q1
     * @param q2
     * @param queryRange
     * @return
     */
    private boolean isQueryConflicting(SingleTableQueryRequest q1, SingleTableQueryRequest q2, List<ColumnQueryRequest> queryRange) {
        // 仅检查 queryRange 之外的列
        List<ColumnQueryRequest> predicates1 = q1.requests().stream()
                .filter(req -> !queryRange.contains(req)) // 仅保留不在 queryRange 内的列
                .toList();

        List<ColumnQueryRequest> predicates2 = q2.requests().stream()
                .filter(req -> !queryRange.contains(req))
                .toList();

        // 如果两个查询涉及的列完全不同，则它们冲突
        Set<String> columns1 = predicates1.stream().map(ColumnQueryRequest::columnName).collect(Collectors.toSet());
        Set<String> columns2 = predicates2.stream().map(ColumnQueryRequest::columnName).collect(Collectors.toSet());

        Set<String> commonColumns = new HashSet<>(columns1);
        commonColumns.retainAll(columns2); // 找到两个查询的交集列

        if (commonColumns.isEmpty()) {
            return true; // 没有任何相同列 => 直接冲突
        }

        // 如果至少有一个相同列的谓词是互斥的，则两个查询不冲突
        for (String column : commonColumns) {
            ColumnQueryRequest p1 = predicates1.stream()
                    .filter(req -> req.columnName().equals(column))
                    .findFirst().orElse(null);

            ColumnQueryRequest p2 = predicates2.stream()
                    .filter(req -> req.columnName().equals(column))
                    .findFirst().orElse(null);

            ColumnTypeAdaptor<?> columnTypeAdaptor=  columnName2Histogram.get(column).getColumnTypeAdaptor();

            if (p1 != null && p2 != null && isMutuallyExclusive(p1, p2, columnTypeAdaptor)) {
                return false; // 发现互斥的谓词 => 不冲突
            }
        }

        // 所有相同列上的谓词都不互斥，则冲突
        return true;
    }

    /**
     * 判断在同一属性列上的两个谓词范围是否有冲突（即，overlap）
     * @param p1
     * @param p2
     * @return
     */
    private boolean isMutuallyExclusive(ColumnQueryRequest p1, ColumnQueryRequest p2, ColumnTypeAdaptor<?> columnTypeAdaptor) {
        // 如果两个谓词是等值查询
        if (p1.rangeType() == RangeType.EQ && p2.rangeType() == RangeType.EQ) {
            return !(columnTypeAdaptor.compare(p1.value(), p2.value()) == 0); // 值不相等 => 互斥
        }

        if ((p1.rangeType() == RangeType.LESS_EQ && p2.rangeType() == RangeType.GREATER_EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) < 0) ||
                (p2.rangeType() == RangeType.LESS_EQ && p1.rangeType() == RangeType.GREATER_EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) > 0)) {
            return true; // 互斥
        }

        // 如果是 "小于" 和 "等于" 互斥
        if ((p1.rangeType() == RangeType.LESS_EQ && p2.rangeType() == RangeType.EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) < 0) ||
                (p2.rangeType() == RangeType.LESS_EQ && p1.rangeType() == RangeType.EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) > 0)) {
            return true;
        }

        // 如果是 "大于" 和 "等于" 互斥
        if ((p1.rangeType() == RangeType.GREATER_EQ && p2.rangeType() == RangeType.EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) > 0) ||
                (p2.rangeType() == RangeType.GREATER_EQ && p1.rangeType() == RangeType.EQ && columnTypeAdaptor.compare(p1.value(), p2.value()) < 0)) {
            return true;
        }

        return false; // 没有发现互斥的情况
    }

    public void saveQErrorResults(String filenamePrefix,
                                  List<SingleTableQueryRequest> singleTableQueryRequests,
                                  long[] estCard,
                                  double[] qErrors,
                                  long[] queryLatency) {
        // 生成时间戳，例如 2025-04-07_15-30-45
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        String filename = filenamePrefix + timestamp + ".csv";

        try (FileWriter csvWriter = new FileWriter(filename)) {
            csvWriter.append("Index,Query,trueCard,estCard,qError,latency_ms\n");
            for (int i = 0; i < singleTableQueryRequests.size(); i++) {
                csvWriter.append(String.format("%d,\"%s\",%d,%d,%.6f,%d\n",
                        i,
                        singleTableQueryRequests.get(i).requests().toString().replace("\"", "\"\""),
                        singleTableQueryRequests.get(i).cardinality(),
                        estCard[i],
                        qErrors[i],
                        queryLatency[i]));
            }
            csvWriter.flush();
            logger.info("qError results saved to {}", filename);
        } catch (IOException e) {
            logger.error("Error writing to CSV file: {}", e.getMessage());
        }
    }

    public void saveQErrorDetailResults(String filenamePrefix,
                                        List<SingleTableQueryRequest> singleTableQueryRequests,
                                        long[] estCard,
                                        double[] qErrors,
                                        List<HistogramAnalysisResult> analysisResults,
                                        long[] queryLatency) {

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        String filename = filenamePrefix + "detail_" + timestamp + ".csv";

        try (FileWriter csvWriter = new FileWriter(filename)) {
            csvWriter.append("Index,Query,trueCard,estCard,qError,latency_ms,SelectedHistograms,UncoveredColumns,CommonColumns,ForwardOverlapColumns,CoveredColumns\n");

            for (int i = 0; i < singleTableQueryRequests.size(); i++) {
                SingleTableQueryRequest request = singleTableQueryRequests.get(i);
                HistogramAnalysisResult analysis = analysisResults.get(i);

                String selectedHistogramsStr = analysis.selectedHistograms().stream()
                        .map(h -> String.join(",", h.getColumnNames()))
                        .collect(Collectors.joining(" | "));

                String uncoveredColumnsStr = String.join(" | ", analysis.uncoveredColumns());
                String commonColumnsStr = String.join(" | ", analysis.commonColumns());
                String forwardOverlapStr = analysis.forwardOverlappingColumns().stream()
                        .map(set -> String.join("&", set))
                        .collect(Collectors.joining(" | "));
                String coveredColumnsStr = String.join(" | ", analysis.coveredColumns());

                csvWriter.append(String.format("%d,\"%s\",%d,%d,%.6f,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
                        i,
                        request.requests().toString().replace("\"", "\"\""),
                        request.cardinality(),
                        estCard[i],
                        qErrors[i],
                        queryLatency[i],
                        selectedHistogramsStr,
                        uncoveredColumnsStr,
                        commonColumnsStr,
                        forwardOverlapStr,
                        coveredColumnsStr));
            }

            csvWriter.flush();
            logger.info("Detailed qError results saved to {}", filename);
        } catch (IOException e) {
            logger.error("Error writing detailed CSV file: {}", e.getMessage());
        }
    }

    public void saveQErrorDetailResultsForTrain(String filenamePrefix,
                                        List<SingleTableQueryRequest> singleTableQueryRequests,
                                        double[] qErrors) {

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        String filename = filenamePrefix + "detail_" + timestamp + ".csv";

        try (FileWriter csvWriter = new FileWriter(filename)) {
            csvWriter.append("Index,Query,trueCard,qError\n");

            for (int i = 0; i < singleTableQueryRequests.size(); i++) {
                SingleTableQueryRequest request = singleTableQueryRequests.get(i);

                csvWriter.append(String.format("%d,\"%s\",%d,%.6f\n",
                        i,
                        request.requests().toString().replace("\"", "\"\""),
                        request.cardinality(),
                        qErrors[i]));
            }

            csvWriter.flush();
            logger.error("Detailed train qError results saved to {}", filename);
        } catch (IOException e) {
            logger.error("Error writing detailed CSV file: {}", e.getMessage());
        }
    }

    public void printQErrorStatistics(List<SingleTableQueryRequest> singleTableQueryRequests, double[] qErrors, long[] estCard) {
        double[] sortedQErrors = qErrors.clone();
        Arrays.sort(sortedQErrors);

        // 计算分位数
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (double q : sortedQErrors) {
            stats.addValue(q);
        }

        double[] percentiles = {1, 25, 50, 75, 90, 95, 99, 99.9};
        for (double p : percentiles) {
            double percentileValue = stats.getPercentile(p);
            logger.info("{}-th qError is {}", p, percentileValue);
        }

        logger.info("avg qError is {}", stats.getMean());

        // 打印超过99.9分位数的qError对应的query信息
        double per999QError = stats.getPercentile(99.9);
        for (int i = 0; i < singleTableQueryRequests.size(); i++) {
            if (qErrors[i] >= per999QError) {
                logger.debug("{}-th, qError: {}, query: {}, TrueCard: {}, EstCard: {}", i, qErrors[i],
                        singleTableQueryRequests.get(i),
                        singleTableQueryRequests.get(i).cardinality(),
                        estCard[i]); // 这里直接用传入的estCard数组
            }
        }
    }
    public void printQErrorStatisticsForTrain(double[] qErrors) {
        double[] sortedQErrors = qErrors.clone();
        Arrays.sort(sortedQErrors);

        // 计算分位数
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (double q : sortedQErrors) {
            stats.addValue(q);
        }

        double[] percentiles = {1, 25, 50, 75, 90, 95, 99, 99.9};
        for (double p : percentiles) {
            double percentileValue = stats.getPercentile(p);
            logger.info("{}-th qError is {}", p, percentileValue);
        }

        logger.info("avg qError is {}", stats.getMean());
    }


    /**
     * 选取当前查询使用的直方图
     * @param selectedHistograms 选取的多维直方图
     * @param uncoveredColumns 查询中未被多维直方图覆盖的列
     * @param commonColumns 在多维直方图中出现次数>=2次的列
     * @param forwardOverlappingColumns 当前多维直方图与前面的多维直方图中共有的列（需要作为条件概率的条件列）
     * @param coveredColumns 选取的多维直方图中涉及的列
     */
    public record HistogramAnalysisResult(
            List<MultiColumnHistogram> selectedHistograms,
            List<String> uncoveredColumns,
            List<String> commonColumns,
            List<Set<String>> forwardOverlappingColumns,
            List<String> coveredColumns
    ) {}

    public HistogramAnalysisResult analyzeSelectedHistograms(Set<String> queryColumns) {
        List<MultiColumnHistogram> selectedHistograms = new ArrayList<>();
        Set<String> remainingColumns = new HashSet<>(queryColumns);
        List<MultiColumnHistogram> availableHistograms = new ArrayList<>(multiColumnHistograms);
        Set<String> allSelectedColumns = new HashSet<>();
        List<MultiColumnHistogram> toRemove = new ArrayList<>();

        while (!remainingColumns.isEmpty() && !availableHistograms.isEmpty()) {
            MultiColumnHistogram bestHistogram = null;
            int maxCoverage = 0;
            int maxOverlap = -1;

            toRemove.clear();
            for (MultiColumnHistogram histogram : availableHistograms) {
                Set<String> intersection = new HashSet<>(histogram.getColumnNames());
                intersection.retainAll(remainingColumns);
                int coverage = intersection.size();

                if (coverage == 0) {
                    toRemove.add(histogram);
                    continue;
                }

                if (coverage == 1 && !selectedHistograms.isEmpty()) {
                    boolean overlapsWithSelected = !Collections.disjoint(histogram.getColumnNames(), allSelectedColumns);
                    if (!overlapsWithSelected) continue;
                }

                Set<String> overlap = new HashSet<>(histogram.getColumnNames());
                overlap.retainAll(allSelectedColumns);
                int overlapWithSelected = overlap.size();

                if (overlapWithSelected > maxOverlap ||
                        (overlapWithSelected == maxOverlap && coverage > maxCoverage)) {
                    bestHistogram = histogram;
                    maxOverlap = overlapWithSelected;
                    maxCoverage = coverage;
                }
            }

            // 批量移除 coverage == 0 的直方图，避免迭代时修改集合
            if (!toRemove.isEmpty()) availableHistograms.removeAll(toRemove);

            if (bestHistogram == null) break;

            selectedHistograms.add(bestHistogram);
            availableHistograms.remove(bestHistogram);
            remainingColumns.removeAll(bestHistogram.getColumnNames());
            // 增量更新 allSelectedColumns
            allSelectedColumns.addAll(bestHistogram.getColumnNames());
        }
        // 保持选取的直方图与建立时的偏序关系
//        selectedHistograms.sort(Comparator.comparingInt(multiColumnHistograms::indexOf));

        // 所选直方图覆盖的列
        Set<String> coveredColumnSet = selectedHistograms.stream()
                .flatMap(h -> h.getColumnNames().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<String> coveredColumns = new ArrayList<>(coveredColumnSet);

        // 所选直方图未覆盖的列
        List<String> uncoveredColumns = new ArrayList<>(remainingColumns);
        Map<String, Integer> columnFrequency = new HashMap<>();
        for (MultiColumnHistogram hist : selectedHistograms) {
            for (String col : hist.getColumnNames()) {
                columnFrequency.put(col, columnFrequency.getOrDefault(col, 0) + 1);
            }
        }
        List<String> commonColumns = columnFrequency.entrySet()
                .stream()
                .filter(entry -> entry.getValue() >= 2)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // 前向重叠列
        List<Set<String>> forwardOverlappingColumns = new ArrayList<>();
        for (int i = 0; i < selectedHistograms.size(); i++) {
            MultiColumnHistogram current = selectedHistograms.get(i);
            Set<String> overlap = new HashSet<>();
            for (int j = 0; j < i; j++) {
                MultiColumnHistogram previous = selectedHistograms.get(j);
                Set<String> intersection = new HashSet<>(current.getColumnNames());
                intersection.retainAll(previous.getColumnNames());
                overlap.addAll(intersection);
            }
            forwardOverlappingColumns.add(overlap);
        }

        // Sanity Check:
        // 如果最终只选了一个直方图 且只能覆盖1个查询列，则认为直方图无效
        if (selectedHistograms.size() == 1) {
            MultiColumnHistogram only = selectedHistograms.get(0);
            Set<String> intersection = new HashSet<>(only.getColumnNames());
            intersection.retainAll(queryColumns);
            if (intersection.size() == 1) {
                selectedHistograms.clear();
                uncoveredColumns = new ArrayList<>(queryColumns); // 全部未覆盖
                commonColumns.clear();
                forwardOverlappingColumns.clear();
                coveredColumns.clear();
            }
        }

        return new HistogramAnalysisResult(
                selectedHistograms,
                uncoveredColumns,
                commonColumns,
                forwardOverlappingColumns,
                coveredColumns
        );
    }



    /**
     * 获取查询在单列上的请求
     */
    public List<BucketOfSingleColumn> getSingleColumnBuckets(String columnName, Map<String, ColumnQueryRequest> columnRequestMap) {
        var histogram = columnName2Histogram.get(columnName);
        ColumnQueryRequest query = columnRequestMap.get(columnName);

        if (query != null) {
            return histogram.getSingleColumnBucketsFromRequest(query.value(), query.rangeType());
        } else {
            return histogram.getSingleColumnBucketsOfWholeDomain();
        }
    }

    /**
     * 枚举所有公共列组合，返回每个组合为 Map<列名, 桶>
     */
    private List<Map<String, BucketOfSingleColumn>> enumerateCommonColumnBucketCombinations(
            List<String> commonColumns,
            Map<String, List<BucketOfSingleColumn>> query2Buckets) {

        List<List<BucketOfSingleColumn>> bucketLists = commonColumns.stream()
                .map(query2Buckets::get)
                .collect(Collectors.toList());

        // =====================================================
        // 关键修复：任一公共列无可选 bucket → 无组合
        // =====================================================
        for (List<BucketOfSingleColumn> buckets : bucketLists) {
            if (buckets == null || buckets.isEmpty()) {
                return Collections.emptyList();
            }
        }

        List<Map<String, BucketOfSingleColumn>> result = new ArrayList<>();

        int dims = commonColumns.size();
        int[] idx = new int[dims];
        int[] sizes = bucketLists.stream().mapToInt(List::size).toArray();

        while (true) {
            Map<String, BucketOfSingleColumn> comb = new HashMap<>();
            for (int i = 0; i < dims; i++) {
                comb.put(commonColumns.get(i), bucketLists.get(i).get(idx[i]));
            }
            result.add(comb);

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
     * 使用多维直方图估计查询基数，用于测试查询
     * @param singleTableQueryRequests
     * @param isPrint
     * @param isSave
     * @return
     */
    public void computeTrainQErrorUseMultiHisFastParallel(List<SingleTableQueryRequest> singleTableQueryRequests, boolean isPrint, boolean isSave, ExecutorService executor) {
        logger.info("begin compute qError for testing");
        //long methodStart = System.currentTimeMillis();

        double[] qErrorsFromMultiHis = new double[singleTableQueryRequests.size()];
        long[] resultsFromMultiHis = new long[singleTableQueryRequests.size()];
        long[] queryLatency = new long[singleTableQueryRequests.size()];

        // 用于debug
        List<HistogramAnalysisResult> analysisResults = new ArrayList<>();

        logger.info("num of MD-Hist: {}", this.multiColumnHistograms.size());
        for (int requestIndex = 0; requestIndex < singleTableQueryRequests.size(); requestIndex++) {
            long startTime = System.currentTimeMillis();

            SingleTableQueryRequest queryRequest = singleTableQueryRequests.get(requestIndex);
            Set<String> queryColumns = queryRequest.getColumnSet();
            Map<String, ColumnQueryRequest> columnQueryMap = queryRequest.mapQueryRequest();

            HistogramAnalysisResult analysisResult = analyzeSelectedHistograms(queryColumns);
            List<MultiColumnHistogram> selectedHistograms = analysisResult.selectedHistograms();
            List<String> uncoveredColumns = analysisResult.uncoveredColumns();
            List<String> commonColumns = analysisResult.commonColumns();
            List<Set<String>> forwardOverlapColumns = analysisResult.forwardOverlappingColumns();
            List<String> coveredColumns = analysisResult.coveredColumns();

            analysisResults.add(analysisResult);

            Map<String, List<BucketOfSingleColumn>> queryColumnBuckets = new HashMap<>();
            for (String column : coveredColumns) {
                queryColumnBuckets.put(column, getSingleColumnBuckets(column, columnQueryMap));
            }

            double probOfQuery;

            if (!selectedHistograms.isEmpty() && commonColumns.isEmpty()) {
                List<Future<Double>> futures = new ArrayList<>();
                for (MultiColumnHistogram hist : selectedHistograms) {
                    futures.add(executor.submit(() -> {
                        List<String> colNames = hist.getColumnNames();
                        List<List<BucketOfSingleColumn>> queryBuckets = new ArrayList<>();
                        for (String col : colNames) {
                            queryBuckets.add(queryColumnBuckets.get(col));
                        }
                        //double[] probs = hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                        //return Arrays.stream(probs).sum();
                        return hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                    }));
                }

                probOfQuery = 1.0;
                for (Future<Double> f : futures) {
                    try {
                        probOfQuery *= f.get();
                    } catch (Exception e) {
                        logger.error("Error computing probOfQuery", e);
                    }
                }

            } else {
                List<Map<String, BucketOfSingleColumn>> commonCombinations = enumerateCommonColumnBucketCombinations(commonColumns, queryColumnBuckets);
                List<Future<Double>> futures = new ArrayList<>();

                for (Map<String, BucketOfSingleColumn> commonBucketMap : commonCombinations) {
                    futures.add(executor.submit(() -> {
                        double product = 1.0;
                        for (int i = 0; i < selectedHistograms.size(); i++) {
                            MultiColumnHistogram hist = selectedHistograms.get(i);
                            List<String> cols = hist.getColumnNames();

                            List<List<BucketOfSingleColumn>> queryBuckets = new ArrayList<>();
                            List<List<BucketOfSingleColumn>> overlapBuckets = new ArrayList<>();

                            for (String col : cols) {
                                if (commonBucketMap.containsKey(col)) {
                                    BucketOfSingleColumn bucket = commonBucketMap.get(col);
                                    queryBuckets.add(List.of(bucket));

                                    if (forwardOverlapColumns.get(i).contains(col)) {
                                        overlapBuckets.add(List.of(bucket));
                                    } else {
                                        overlapBuckets.add(globalDomainBucketCache.get(col));
                                    }
                                } else {
                                    queryBuckets.add(queryColumnBuckets.get(col));
                                    overlapBuckets.add(globalDomainBucketCache.get(col));
                                }
                            }

                            //double[] numerators = hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                            //double[] denominators = hist.getProbabilitiesFromCumulativeIndexWithRatio(overlapBuckets);

                            //double numerator = Arrays.stream(numerators).sum();
                            //double denominator = Arrays.stream(denominators).sum();

                            double numerator = hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                            double denominator = hist.getProbabilitiesFromCumulativeIndexWithRatio(overlapBuckets);
                            double ratio = (denominator > 0) ? numerator / denominator : numerator;

                            product *= Math.min(1.0, ratio);
                        }
                        return product;
                    }));
                }

                probOfQuery = 0.0;
                for (Future<Double> f : futures) {
                    try {
                        probOfQuery += f.get();
                    } catch (Exception e) {
                        logger.error("Error computing commonCombination product", e);
                    }
                }
            }

            for (String col : uncoveredColumns) {
                ColumnQueryRequest req = columnQueryMap.get(col);
                double prob = columnName2Histogram.get(col).getProbability(req.value(), req.rangeType());
                probOfQuery *= prob;
            }

            long latency = System.currentTimeMillis() - startTime;
            queryLatency[requestIndex] = latency;


            long estCardinality = Math.round(probOfQuery * tableSize);
            long estCardinality_ = Math.max(1, estCardinality);

            resultsFromMultiHis[requestIndex] = estCardinality;

            double trueCardinality = queryRequest.cardinality();
            qErrorsFromMultiHis[requestIndex] = Math.max(estCardinality_ / trueCardinality, trueCardinality / estCardinality_);

//            logger.info("query:{}, est:{}, true:{}, q-error:{}", queryRequest.requests(), estCardinality, trueCardinality, qErrorsFromMultiHis[requestIndex]);

//             ✅ 特殊优化: 只有一个MDH 且查询列正好被该MDH覆盖
//            if (multiColumnHistograms.size() > 0) {
//                MultiColumnHistogram onlyHist = multiColumnHistograms.get(multiColumnHistograms.size()-1);
//                Set<String> histColumns = new HashSet<>(onlyHist.getColumnNames());
//
//                if (histColumns.containsAll(queryColumns)) {
//                    double qError = qErrorsFromMultiHis[requestIndex];
//                    if(qError > 1.2){
//                        logger.error("query:{}, est:{}, true:{}, q-error:{}", queryRequest.requests(), estCardinality, trueCardinality, qError);
//                    }else {
//                        logger.info("query:{}, est:{}, true:{}, q-error:{}", queryRequest.requests(), estCardinality, trueCardinality, qError);
//                    }
//                }
//            }
        }
        long totalLatency = Arrays.stream(queryLatency).sum();
        logger.info("Total query latency (ms): " + totalLatency);

        /*
        try {
            String timingPath = "./test_duration/final2.csv";
            File timingFile = new File(timingPath);
            if (timingFile.getParentFile() != null) timingFile.getParentFile().mkdirs();
            boolean newFile = !timingFile.exists();
            try (FileWriter tf = new FileWriter(timingFile, true)) {
                if (newFile) {
                    tf.append("timestamp,method,duration_ms,total_query_latency_ms,num_queries\n");
                }
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss.SSS"));
                long duration = System.currentTimeMillis() - methodStart;
                tf.append(String.format("%s,%s,%d,%d,%d\n", timestamp, "computeTrainQErrorUseMultiHisFastParallel", duration, totalLatency, singleTableQueryRequests.size()));
                tf.flush();
            }
        } catch (IOException e) {
            logger.error("Error writing timing file: {}", e.getMessage());
        }
        */

        if (isSave) {
            saveQErrorDetailResults(Q_ERROR_RESULT_FILE, singleTableQueryRequests, resultsFromMultiHis, qErrorsFromMultiHis, analysisResults, queryLatency);
        }
        if (isPrint) {
            printQErrorStatistics(singleTableQueryRequests, qErrorsFromMultiHis, resultsFromMultiHis);
        }
    }

    /**
     * 计算训练集的Q-error，用于计算训练查询
     * @param singleTableQueryRequests
     * @param isPrint
     * @return
     */
    public TrainQErrorResult computeTrainQErrorUseMultiHisForTrain(List<SingleTableQueryRequest> singleTableQueryRequests, boolean isPrint, ExecutorService executor) {
        logger.info("begin compute qError for training");

        int querySize = singleTableQueryRequests.size();
        double[] localCachedQError = new double[querySize]; // 查询Q-Error
        double[] localCachedEdgeWeight = new double[querySize];   // 施加到CoG的权重

        List<Integer> needComputeIndexes = new ArrayList<>();

        if (addedColumn.isEmpty() && hisColumn.isEmpty()) {
            // 首轮构建的情况
            for (int i = 0; i < querySize; i++) {
                needComputeIndexes.add(i);
            }
        } else if (!addedColumn.isEmpty()) {
            // 存在新增列的情况
            for (int i = 0; i < querySize; i++) {
                Set<String> queryColumns = singleTableQueryRequests.get(i).getColumnSet();
                boolean usesAddedColumns = queryColumns.stream().anyMatch(addedColumn::contains);
                if (!usesAddedColumns) {
                    localCachedEdgeWeight[i] = cachedEdgeWeight[i];
                    localCachedQError[i] = cachedQError[i];
                } else {
                    needComputeIndexes.add(i);
                }
            }
        } else if (!hisColumn.isEmpty()) {
            // 没有新增列的情况
            for (int i = 0; i < querySize; i++) {
                Set<String> queryColumns = singleTableQueryRequests.get(i).getColumnSet();
                boolean usesHisColumns = queryColumns.stream().anyMatch(hisColumn::contains);
                if (!usesHisColumns) {
                    localCachedEdgeWeight[i] = cachedEdgeWeight[i];
                    localCachedQError[i] = cachedQError[i];
                } else {
                    needComputeIndexes.add(i);
                }
            }
        }else {
            logger.error("error in computeTrainQErrorUseMultiHisForTrain");
        }


        for (int requestIndex : needComputeIndexes) {
            SingleTableQueryRequest queryRequest = singleTableQueryRequests.get(requestIndex);
            Set<String> queryColumns = queryRequest.getColumnSet();
            Map<String, ColumnQueryRequest> columnQueryMap = queryRequest.mapQueryRequest();
            HistogramAnalysisResult analysisResult = analyzeSelectedHistograms(queryColumns);

            List<MultiColumnHistogram> selectedHistograms = analysisResult.selectedHistograms();
            List<String> uncoveredColumns = analysisResult.uncoveredColumns();
            List<String> commonColumns = analysisResult.commonColumns();
            List<Set<String>> forwardOverlapColumns = analysisResult.forwardOverlappingColumns();
            List<String> coveredColumns = analysisResult.coveredColumns();

            Map<String, List<BucketOfSingleColumn>> queryBucketsMap = new HashMap<>();
            for (String col : coveredColumns) {
                queryBucketsMap.put(col, getSingleColumnBuckets(col, columnQueryMap));
            }

            double probOfQuery;

            if (!selectedHistograms.isEmpty() && commonColumns.isEmpty()) {
                // 可以并行 selectedHistograms
                List<Future<Double>> futures = new ArrayList<>();
                for (MultiColumnHistogram hist : selectedHistograms) {
                    futures.add(executor.submit(() -> {
                        List<List<BucketOfSingleColumn>> queryBuckets = hist.getColumnNames().stream()
                                .map(queryBucketsMap::get)
                                .collect(Collectors.toList());
                        //return Arrays.stream(hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets)).sum();
                        return hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                    }));
                }

                probOfQuery = 1.0;
                for (Future<Double> future : futures) {
                    try {
                        probOfQuery *= future.get();
                    } catch (Exception e) {
                        logger.error("Error in parallel single-histogram estimation", e);
                    }
                }

            } else {
                List<Map<String, BucketOfSingleColumn>> commonCombinations =
                        enumerateCommonColumnBucketCombinations(commonColumns, queryBucketsMap);

                List<Future<Double>> futures = new ArrayList<>();

                for (Map<String, BucketOfSingleColumn> commonBucketMap : commonCombinations) {
                    futures.add(executor.submit(() -> {
                        double jointProb = 1.0;
                        for (int i = 0; i < selectedHistograms.size(); i++) {
                            MultiColumnHistogram hist = selectedHistograms.get(i);
                            List<String> cols = hist.getColumnNames();

                            List<List<BucketOfSingleColumn>> queryBuckets = new ArrayList<>();
                            List<List<BucketOfSingleColumn>> overlapBuckets = new ArrayList<>();

                            for (String col : cols) {
                                if (commonBucketMap.containsKey(col)) {
                                    List<BucketOfSingleColumn> singleton = List.of(commonBucketMap.get(col));
                                    queryBuckets.add(singleton);
                                    if (forwardOverlapColumns.get(i).contains(col)) {
                                        overlapBuckets.add(singleton);
                                    } else {
                                        overlapBuckets.add(globalDomainBucketCache.get(col));
                                    }
                                } else {
                                    queryBuckets.add(queryBucketsMap.get(col));
                                    overlapBuckets.add(globalDomainBucketCache.get(col));
                                }
                            }

                            //double[] numerators = hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                            //double[] denominators = hist.getProbabilitiesFromCumulativeIndexWithRatio(overlapBuckets);

                            //double numerator = Arrays.stream(numerators).sum();
                            //double denominator = Arrays.stream(denominators).sum();
                            double numerator = hist.getProbabilitiesFromCumulativeIndexWithRatio(queryBuckets);
                            double denominator = hist.getProbabilitiesFromCumulativeIndexWithRatio(overlapBuckets);
                            double ratio = (denominator > 0) ? numerator / denominator : numerator;

                            jointProb *= Math.min(1.0, ratio);
                        }
                        return jointProb;
                    }));
                }

                probOfQuery = 0.0;
                for (Future<Double> future : futures) {
                    try {
                        probOfQuery += future.get();
                    } catch (Exception e) {
                        logger.error("Error in parallel combination estimation", e);
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("result from multiHis:{}", probOfQuery);
                }
            }

            for (String col : uncoveredColumns) {
                ColumnQueryRequest req = columnQueryMap.get(col);
                probOfQuery *= columnName2Histogram.get(col).getProbability(req.value(), req.rangeType());
            }

            long estCardinality = Math.max(1, Math.round(probOfQuery * tableSize));
            double trueCardinality = queryRequest.cardinality();
            double qError = Math.max(estCardinality / trueCardinality, trueCardinality / estCardinality);
            double edgeWeight= (qError - 1) / combineMap.get(queryColumns.size());

            localCachedQError[requestIndex] = qError;
            localCachedEdgeWeight[requestIndex] = edgeWeight;

            if (logger.isDebugEnabled()) {
                logger.debug("query:{}, est:{}, true:{}, q-error:{}",
                        queryRequest.requests(), estCardinality, trueCardinality, qError);
            }
        }

        System.arraycopy(localCachedQError, 0, cachedQError, 0, querySize);
        System.arraycopy(localCachedEdgeWeight, 0, cachedEdgeWeight, 0, querySize);

        if (isPrint) {
            printQErrorStatisticsForTrain(cachedQError);
        }

        return new TrainQErrorResult(cachedQError, cachedEdgeWeight);
    }



    /**
     *
     * @param selectedHistograms 用于构建父约束的直方图
     * @param commonColumns selectedHistograms中出现次数大于等于2的列
     * @param forwardOverlappingColumns 需要作为条件概率中条件的列
     * @param coveredColumns 所有直方图中包含的列
     */
    public record IntervalAnalysisResult(
            List<MultiColumnHistogram> selectedHistograms,
            List<String> commonColumns,
            List<Set<String>> forwardOverlappingColumns,
            List<String> coveredColumns
    ) {}
    public IntervalAnalysisResult analyzeIntervalHistograms(Set<String> intervalColumns) {
        List<MultiColumnHistogram> selectedHistograms = new ArrayList<>();
        Set<String> remainingColumns = new HashSet<>(intervalColumns);

        for (MultiColumnHistogram histogram : multiColumnHistograms) {
            List<String> columnNames = histogram.getColumnNames();
            boolean coversNewColumn = false;

            for (String col : columnNames) {
                if (remainingColumns.contains(col)) {
                    coversNewColumn = true;
                    break;
                }
            }

            if (coversNewColumn) {
                selectedHistograms.add(histogram);
                // 移除所有当前直方图中包含的列
                for (String col : columnNames) {
                    remainingColumns.remove(col);
                }

                // 所有列都覆盖完了就提前返回
                if (remainingColumns.isEmpty()) {
                    break;
                }
            }
        }

        // 直接用 LinkedHashSet 保证顺序
        Set<String> coveredColumnSet = selectedHistograms.stream()
                .flatMap(h -> h.getColumnNames().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<String> coveredColumns = new ArrayList<>(coveredColumnSet);

        // 统计出现频次（commonColumns）
        Map<String, Integer> columnFrequency = new HashMap<>();
        for (MultiColumnHistogram hist : selectedHistograms) {
            for (String col : hist.getColumnNames()) {
                columnFrequency.merge(col, 1, Integer::sum);
            }
        }
        List<String> commonColumns = columnFrequency.entrySet()
                .stream()
                .filter(entry -> entry.getValue() >= 2)
                .map(Map.Entry::getKey)
                .toList(); // Java 16+：可用 List.copyOf(...) 代替

        // 构建 forward overlapping 列
        List<Set<String>> forwardOverlappingColumns = new ArrayList<>();
        Set<String> previousColumns = new HashSet<>();
        for (MultiColumnHistogram hist : selectedHistograms) {
            Set<String> overlap = new HashSet<>();
            for (String col : hist.getColumnNames()) {
                if (previousColumns.contains(col)) {
                    overlap.add(col);
                }
            }
            forwardOverlappingColumns.add(overlap);
            previousColumns.addAll(hist.getColumnNames());
        }

        return new IntervalAnalysisResult(
                selectedHistograms,
                commonColumns,
                forwardOverlappingColumns,
                coveredColumns
        );
    }

    /**
     * 获取目标列 targetColumns 的联合概率分布，使用多个直方图构建条件概率。
     * 使用 queryIndexes 和 overlapIndexes 构造每个直方图的条件与联合概率索引集合。
     */
    private double[] computeParentProbFromMultiHis(List<String> targetColumns) {
        IntervalAnalysisResult intervalAnalysisResult = analyzeIntervalHistograms(new HashSet<>(targetColumns));
        List<MultiColumnHistogram> selectedHistograms = intervalAnalysisResult.selectedHistograms();
        List<String> commonColumns = intervalAnalysisResult.commonColumns();
        List<Set<String>> forwardOverlapColumns = intervalAnalysisResult.forwardOverlappingColumns();
        List<String> coveredColumns = intervalAnalysisResult.coveredColumns(); // 所有的列

        Set<String> outerColumns = new LinkedHashSet<>(targetColumns);
        outerColumns.addAll(commonColumns);
        List<String> allOuterColumns = new ArrayList<>(outerColumns); // 需要对齐的列

        // 所有的列对应的桶
        Map<String, List<Integer>> column2index = new HashMap<>();
        for (String col : coveredColumns) {
            column2index.put(col, IntStream.range(0, columnName2Histogram.get(col).getHistogramSize())
                    .boxed().collect(Collectors.toList()));
        }

        int[] cumulativeDim = new int[targetColumns.size()];
        int totalSize = 1;
        for (int i = targetColumns.size() - 1; i >= 0; i--) {
            cumulativeDim[i] = totalSize;
            totalSize *= column2index.get(targetColumns.get(i)).size();
        }

        double[] result = new double[totalSize];
        List<Map<String, Integer>> allIndexCombinations = enumerateIntervalBucketCombinations(allOuterColumns, column2index);

        allIndexCombinations.parallelStream().forEach(indexMap -> {
            double prob = selectedHistograms.parallelStream().mapToDouble(selectedHistogram -> {
                int histIdx = selectedHistograms.indexOf(selectedHistogram);
                List<String> histCols = selectedHistogram.getColumnNames();

                List<List<Integer>> queryIndexes = new ArrayList<>();
                List<List<Integer>> overlapIndexes = new ArrayList<>();

                for (String col : histCols) {
                    if (indexMap.containsKey(col)) {
                        int idx = indexMap.get(col);
                        queryIndexes.add(List.of(idx));

                        if (forwardOverlapColumns.get(histIdx).contains(col)) {
                            overlapIndexes.add(List.of(idx));
                        } else {
                            List<Integer> full = new ArrayList<>(column2index.get(col));
                            overlapIndexes.add(full);
                        }
                    } else {
                        List<Integer> full = new ArrayList<>(column2index.get(col));
                        queryIndexes.add(full);
                        overlapIndexes.add(full);
                    }
                }

                double[] numerators = selectedHistogram.getProbabilitiesFromCumulativeIndex(queryIndexes);
                double[] denominators = selectedHistogram.getProbabilitiesFromCumulativeIndex(overlapIndexes);

                double numerator = Arrays.stream(numerators).sum();
                double denominator = Arrays.stream(denominators).sum();

                return Math.min(1.0, (denominator > 0 ? numerator / denominator : numerator));
            }).reduce(1.0, (a, b) -> a * b);

            int index = 0;
            for (int i = 0; i < targetColumns.size(); i++) {
                index += indexMap.get(targetColumns.get(i)) * cumulativeDim[i];
            }
            synchronized (result) {
                result[index] += prob;
            }
        });

        return result;
    }


    /**
     * 枚举所有组合，返回每个组合为 Map<列名, 桶索引>
     */
    private List<Map<String, Integer>> enumerateIntervalBucketCombinations(
            List<String> allColumns,
            Map<String, List<Integer>> column2index) {

        List<List<Integer>> indexLists = allColumns.stream()
                .map(column2index::get)
                .collect(Collectors.toList());

        List<Map<String, Integer>> result = new ArrayList<>();
        int dims = allColumns.size();
        int[] idx = new int[dims];
        int[] sizes = indexLists.stream().mapToInt(arr -> arr.size()).toArray();

        while (true) {
            Map<String, Integer> comb = new HashMap<>();
            for (int i = 0; i < dims; i++) {
                comb.put(allColumns.get(i), indexLists.get(i).get(idx[i]));
            }
            result.add(comb);

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
     * 打印用于设置约束的查询信息
     * @throws IOException
     */
    public void exportQueryUsageInfoToFiles() throws IOException {
        logger.debug("equalityConstraintQueries size: " + equalityConstraintQueries.size());
        logger.debug("lowerBoundConstraintQueries size: " + lowerBoundConstraintQueries.size());

        // 构建输出结构
        Map<String, Set<Integer>> output = new HashMap<>();
        output.put("equalityConstraintQueries", equalityConstraintQueries);
        output.put("lowerBoundConstraintQueries", lowerBoundConstraintQueries);

        // 序列化并写入 JSON 文件
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(Paths.get("./output/constraint_queries.json").toFile(), output);
    }

    /**
     * 存储单列和多列直方图的信息，并打印物理大小
     * @return
     */
    public void exportMultiHistogramStorageInfoToFiles() {
        double multiTotal = 0.0;

        // === Multi-column histograms ===
        for (MultiColumnHistogram mch : multiColumnHistograms) {
            double size = mch.getMDHistSizeInKB();
            multiTotal += size;
            logger.info("Multi-column [{}] size: {} KB", String.join(", ", mch.getColumnNames()), String.format("%.2f", size));
        }

        logger.info("== Summary ==");
        logger.info("Total single-column histogram size: {} KB", String.format("%.2f", this.SingleHistTotalSizeInKB));
        logger.info("Total multi-column histogram size: {} KB", String.format("%.2f", multiTotal));
        logger.info("Total histogram storage size: {} KB", String.format("%.2f", (this.SingleHistTotalSizeInKB + multiTotal)));
    }

    /**
     * 统计单列直方图大小
     */
    public void exportSingleHistogramStorageInfoToFiles() {
        ObjectMapper mapper = new ObjectMapper();
        double singleTotal = 0;

        // === 单列直方图 ===
        for (Map.Entry<String, SingleColumnHistogram<?>> entry : columnName2Histogram.entrySet()) {
            String col = entry.getKey();
            Map<String, Object> info = entry.getValue().exportStorageInfo();
            String filePath = String.format("./output/single_column_%s.json", col);

            double size = writeToJsonAndMeasure(mapper, info, filePath) / 1024.0 ;
            singleTotal += size;

            logger.info("Single column [{}] size: {} KB", col, String.format("%.2f", size));
        }

        this.SingleHistTotalSizeInKB = singleTotal;

        logger.info("Total single-column histogram size: {} KB", String.format("%.2f", singleTotal));
    }

    private long writeToJsonAndMeasure(ObjectMapper mapper, Map<String, Object> info, String path) {
        try (FileWriter writer = new FileWriter(path)) {
            mapper.writeValue(writer, info);
            return new File(path).length();
        } catch (IOException e) {
            logger.error("写入失败: {}", path, e);
            return 0;
        }
    }
}
