package ecnu.db;

import ecnu.db.correlation.ColumnCorrelationFinder;
import ecnu.db.histogram.HistogramManager;
import ecnu.db.histogram.single.SingleColumnHistogram;
import ecnu.db.queylog.single.ColumnQueryRequest;
import ecnu.db.queylog.single.SingleTableQueryLog;
import ecnu.db.queylog.single.SingleTableQueryRequest;
import ecnu.db.utils.ColumnHistogramBundle;
import ecnu.db.utils.ConstructorForCSV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Metis {
    private static final Logger logger = LoggerFactory.getLogger(Metis.class);

    // ================== 基础配置 ==================
    private static final int SAMPLE_SIZE = 3000;           
    private static final int THREAD_POOL_SIZE = 96;        

    // ================== 早停配置 ==================
    private static final double MIN_IMPROVEMENT_RATIO = 0.01; 
    private static final int MAX_STAGNANT_ROUNDS = 2;          
    private static final int MIN_ROUNDS_BEFORE_STOP = 2;       

    // ================== 早停状态变量 ==================
    private static double prevCompositeScore = Double.MAX_VALUE;
    private static int stagnantRounds = 0;


    public static void main(String[] args) throws IOException {

        boolean ENABLE_STORAGE_LIMIT = false; // 是否限制MD-Hist直方图存储上限 
        double MDHist_SIZE_LIMIT_KB = 0.0;    // MD-Hist直方图存储上限（当ENABLE_STORAGE_LIMIT为true时有效）

        boolean isWithHeader = false; // csv文件是否包含表头

        if (args.length < 3) {
            logger.error("参数数量不足，应为：trainFile testFile csvFile [--withHeader] [--limit=sizeKB]");
            System.exit(-1);
        }

        // ===== 基本参数 =====
        String trainQueryFileName = args[0];
        String testQueryFileName = args[1];
        String csvFileName = args[2];

        // ===== 可选参数解析 =====
        for (int i = 3; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("--withHeader")) {
                isWithHeader = true;
            } else if (arg.startsWith("--limit")) {
                ENABLE_STORAGE_LIMIT = true;
                String[] parts = arg.split("=");
                if (parts.length == 2) {
                    try {
                        MDHist_SIZE_LIMIT_KB = Double.parseDouble(parts[1]);
                    } catch (NumberFormatException e) {
                        logger.error("非法的 --limit 参数值: {}", parts[1]);
                        System.exit(-1);
                    }
                } else {
                    logger.error("用法错误: --limit=<size_in_KB>");
                    System.exit(-1);
                }
            } else {
                logger.warn("未知参数被忽略: {}", arg);
            }
        }

        logger.debug("Sample size: {}", SAMPLE_SIZE);
        logger.info("CSV contains header (--withHeader): {}", isWithHeader);
        logger.info("Storage limit enabled (--limit): {}", ENABLE_STORAGE_LIMIT);
        if (ENABLE_STORAGE_LIMIT) {
            logger.info("Storage limit: {} KB", MDHist_SIZE_LIMIT_KB);
        }

        // 读取训练查询
        var trainQueryRequests = SingleTableQueryLog.readSingleTableQueryRequests(trainQueryFileName);
        logger.info("train requests are {}", trainQueryRequests.size());

        //读取测试查询
        var testQueryRequests = SingleTableQueryLog.readSingleTableQueryRequests(testQueryFileName);
        logger.info("test requests are {}", testQueryRequests.size());

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // 提取所有查询中涉及到的单列
        Set<String> involvedColumns = trainQueryRequests.stream().flatMap(singleTableQueryRequest -> singleTableQueryRequest.requests().stream()).map(ColumnQueryRequest::columnName).collect(Collectors.toSet());

        // 构建或加载单列直方图
        String histogramPath = "./Expr_Data/dump/" + csvFileName + "_column_histograms.ser";
        File file = new File(histogramPath);
        ColumnHistogramBundle bundle;
        if (file.exists()) {
            try {
                logger.info("Loading column histograms from cache...");
                bundle = loadColumnHistogramBundle(histogramPath);
            } catch (Exception e) {
                logger.error("Failed to load histograms from file. Will reconstruct. Cause: {}", e.getMessage());
                file.delete();
                bundle = ConstructorForCSV.construct(involvedColumns, csvFileName, isWithHeader);
                saveColumnHistogramBundle(bundle, histogramPath);
            }
        } else {
            logger.info("Construct column histograms...");
            bundle = ConstructorForCSV.construct(involvedColumns, csvFileName, isWithHeader);
            file.getParentFile().mkdirs();
            logger.info("Cache column histograms to {}", histogramPath);
            saveColumnHistogramBundle(bundle, histogramPath);
        }
        // 单个属性列的数据分布信息
        SequencedMap<String, SingleColumnHistogram<?>> columnHistogramMap = bundle.getHistogramMap();
        // 数据行数
        int totalRow = bundle.getTotalRow();
        logger.info("total row:{}", totalRow);

        double totalCumulativeDomainSize = 1;
        for (var columnNameAndHistogram : columnHistogramMap.entrySet()) {
            var histogram = columnNameAndHistogram.getValue();
            logger.info("column name: {}, ndv:{}, mcv:{}, merged hitogram size:{}, accuracy deviation: {} ",
                    columnNameAndHistogram.getKey(), histogram.getNdvSize(), histogram.getMcvSize(), histogram.getHistogramSize(), histogram.getEachRangeProbability());
            histogram.debugPrintHistogramBySemantic();
            totalCumulativeDomainSize *= histogram.getNdvSize();
        }
        logger.info("total cumulative domain size: {}", totalCumulativeDomainSize);

        logger.info("clean single column queries");
        var trainQueryRequestsWithMultipleColumns = trainQueryRequests.stream().filter(singleTableQueryRequest -> singleTableQueryRequest.requests().size() > 1).toList();
        if (trainQueryRequestsWithMultipleColumns.isEmpty()) {
            logger.warn("no multiple column queries, output data from the single column histogram");
            return;
        } else {
            logger.info("total train requests with multiple columns are {}", trainQueryRequestsWithMultipleColumns.size());
        }

        // 采样训练查询，用于关联列出挖掘
        int sampleSize = Math.min(trainQueryRequestsWithMultipleColumns.size(), SAMPLE_SIZE);
        HistogramManager histogramManager = new HistogramManager(columnHistogramMap, totalRow, trainQueryRequestsWithMultipleColumns, sampleSize);
        ColumnCorrelationFinder columnCorrelationFinder = new ColumnCorrelationFinder(columnHistogramMap, trainQueryRequestsWithMultipleColumns);
        List<SingleTableQueryRequest> sampledTrainQueries = trainQueryRequestsWithMultipleColumns.subList(0, sampleSize);
        Set<Set<String>> constructedAttributeSubsets = new HashSet<>();
        histogramManager.computeTrainQErrorUseMultiHisFastParallel(testQueryRequests, true,  false, executor);

        logger.info("begin construct multiple column histogram");
        long startTime = System.currentTimeMillis();
        int round = 0;
        do {
            round++;
            logger.info("Round {}: single statistic(kb):{}, MD-Hist statistic(kb):{}, total statistic(kb):{}", round, histogramManager.SingleHistTotalSizeInKB, histogramManager.MDHistTotalSizeInKB, histogramManager.MDHistTotalSizeInKB + histogramManager.SingleHistTotalSizeInKB);

            // 计算当前轮次，训练查询的Q-error
            HistogramManager.TrainQErrorResult trainResult = histogramManager.computeTrainQErrorUseMultiHisForTrain(sampledTrainQueries, false, executor);
            double[] trainQErrors = trainResult.qErrors;
            double[] edgeWeights = trainResult.edgeWeights;

            // 早停逻辑
            if (shouldEarlyStop(trainQErrors, round)) {
                logger.warn("Early stopping triggered after {} rounds.", round);
                break;
            }

            // 属性子集选取
            List<String> chosenColumns = columnCorrelationFinder.analyzeCorrelation(sampledTrainQueries, edgeWeights, constructedAttributeSubsets);

            // MD Hist构建
            histogramManager.constructHistogram(chosenColumns);

            // 记录已经构建过的列簇子集
            constructedAttributeSubsets.add(new HashSet<>(chosenColumns));

            histogramManager.exportQueryUsageInfoToFiles();
        }  while (!isBudgetExceeded(histogramManager, ENABLE_STORAGE_LIMIT, MDHist_SIZE_LIMIT_KB));
        logger.info("time since start: {} ms", System.currentTimeMillis() - startTime);
        logger.info("compute the final qError for test query");
        histogramManager.computeTrainQErrorUseMultiHisFastParallel(testQueryRequests, true,  true, executor);
        histogramManager.exportMultiHistogramStorageInfoToFiles();
        executor.shutdown();
    }

    public static void saveColumnHistogramBundle(ColumnHistogramBundle bundle, String path) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
            oos.writeObject(bundle);
        }
    }

    public static ColumnHistogramBundle loadColumnHistogramBundle(String path) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
            return (ColumnHistogramBundle) ois.readObject();
        }
    }

    /**
     * 判断当前MD-Hist和单列直方图的总存储大小是否超过预设的限制
     * @param histogramManager
     * @param enableLimit
     * @param limitKB
     * @return
     */
    public static boolean isBudgetExceeded(HistogramManager histogramManager, boolean enableLimit, double limitKB) {
        if (!enableLimit) return false;
        double totalSize = histogramManager.MDHistTotalSizeInKB + histogramManager.SingleHistTotalSizeInKB;
        if (totalSize >= limitKB) {
            logger.warn("Storage limit reached ({} KB). Current total size is {:.2f} KB. Stopping construction.", limitKB, totalSize);
            return true;
        }
        return false;
    }

    /**
     * 根据当前轮训练查询的 Q-error 判断是否应当提前停止
     *
     * @param qErrors 当前轮的 Q-error 数组
     * @param round   当前迭代轮次
     * @return true 表示应停止迭代；false 表示继续
     */
    private static boolean shouldEarlyStop(double[] qErrors, int round) {
        double[] sorted = qErrors.clone();
        Arrays.sort(sorted);

        int n = sorted.length;

        double p50 = sorted[(int)(n * 0.5)];
        double p90 = sorted[(int)(n * 0.9)];
        double p95 = sorted[(int)(n * 0.95)];


        double compositeScore = (p50 + p90 + p95) / 3.0;

        logger.info(String.format(
                Locale.US,
                "Round %d - Q-error stats: p50=%.4f, p90=%.4f, p95=%.4f, composite=%.4f",
                round, p50, p90, p95, compositeScore
        ));


        if (p95 <= 1.0 + 1e-9) {
            logger.warn(
                    "Early stop: all training queries are perfectly estimated "
                            + "(p95 q-error = 1.0)."
            );
            return true;
        }

        boolean shouldStop = false;
        if (prevCompositeScore < Double.MAX_VALUE) {
            double improvementRatio = (prevCompositeScore - compositeScore) / prevCompositeScore;
            logger.info("improvementRatio: {}", improvementRatio);
            if (improvementRatio < MIN_IMPROVEMENT_RATIO) {
                stagnantRounds++;
                logger.warn(String.format(
                        Locale.US,
                        "Improvement too small (%.4f), stagnantRounds=%d",
                        improvementRatio, stagnantRounds
                ));
            } else {
                stagnantRounds = 0;
            }

            if (round >= MIN_ROUNDS_BEFORE_STOP && stagnantRounds >= MAX_STAGNANT_ROUNDS) {
                shouldStop = true;
                logger.warn("Early stopping triggered after {} stagnant rounds.", stagnantRounds);
            }
        }

        prevCompositeScore = compositeScore;
        return shouldStop;
    }
}
