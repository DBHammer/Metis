package ecnu.db.correlation;

import ecnu.db.histogram.single.SingleColumnHistogram;
import ecnu.db.queylog.single.ColumnQueryRequest;
import ecnu.db.queylog.single.SingleTableQueryRequest;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.correlation.ColumnCorrelationCPSolver.chooseCorrelatedColumnIndex;

public class ColumnCorrelationFinder {
    private static final Logger logger = LoggerFactory.getLogger(ColumnCorrelationFinder.class);

    private static final String QUERY_Q_ERROR_FILE_NAME = "./tmp/qError.txt";
    private static final String COLUMN_VALUE_FREQUENCY_ITEM_SET = "./tmp/columnFrequencyItem.txt";

    // 由于frequency是累加值，会受到查询个数影响。所以，需要知道对该列对施加q-error的查询个数。进而，sum(frequency)/num 来进行均一化
    private static final String QUERY_WEIGHT_FILE_NAME = "./tmp/queryNum.txt";
    private static final String COLUMN_WEIGHT_FREQUENCY_ITEM_SET = "./tmp/columnPairWeight.txt";

    private static final double MAX_Q_ERROR_FILE_ROW = 1e3;
    private static final double MIN_SUP = 0.00001;  // 频繁项挖掘的最小支持度
    private static final int DEBUG_OUTPUT_NUM = 100;
    private static final int CORRELATION_COLUMN_NUM = 2;
    private static final int FREQUENCY_ITEM_SET_SIZE = 2;

    private final String[] columnNames;
    private final int[] histogramSize;
    private final List<ColumnQueryRequest> columnRequests; //所有查询中的单个非重复谓词

    public ColumnCorrelationFinder(Map<String, SingleColumnHistogram<?>> columnHistogramMap,
                                   List<SingleTableQueryRequest> singleTableQueryRequests) {
        columnNames = new String[columnHistogramMap.size()];
        histogramSize = new int[columnHistogramMap.size()];
        int index = 0;
        for (var columnNameAndHistogram : columnHistogramMap.entrySet()) {
            columnNames[index] = columnNameAndHistogram.getKey();
            histogramSize[index] = columnNameAndHistogram.getValue().getHistogramSize();
            index++;
        }
        columnRequests = singleTableQueryRequests.stream()
                .flatMap(singleTableQueryRequest -> singleTableQueryRequest.requests().stream()).distinct().toList();
    }

    /**
     * CP1
     *
     * @param singleTableQueryRequests   查询请求
     * @param qErrors                    查询请求的误差
     * @return 相关的列
     */
    public List<String> analyzeCorrelation(
            List<SingleTableQueryRequest> singleTableQueryRequests,
            double[] qErrors,
            Set<Set<String>> alreadyConstructedAttributeSubsets) throws IOException {

        File tmpDir = new File("./tmp/");
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }

        List<Integer> chosenColumnIndex;


        // 挖掘Q-ERROR
        writeQErrorFile(singleTableQueryRequests, qErrors);
        AlgoNegFIN algoNegFIN = new AlgoNegFIN();
        algoNegFIN.runAlgorithm(QUERY_Q_ERROR_FILE_NAME, MIN_SUP, COLUMN_VALUE_FREQUENCY_ITEM_SET);
        algoNegFIN.printStats();
        var columnRequest2Frequency = readFrequencyItemSet();

        // 挖掘查询权重。和上一步挖掘的Frequency相除，从而获取单位误差。
        writeQueryWeightFile(singleTableQueryRequests);
        algoNegFIN.runAlgorithm(QUERY_WEIGHT_FILE_NAME, MIN_SUP, COLUMN_WEIGHT_FREQUENCY_ITEM_SET);
        algoNegFIN.printStats();
        var columnPairIndex2Weight = readColumnWeightFrequencyItemSet();

        Map<Set<Integer>, Integer> columnPair2Frequency = extractColumnPair2Frequency(columnRequest2Frequency, columnPairIndex2Weight, alreadyConstructedAttributeSubsets);
        Set<Set<Integer>> alreadyConstructedIndexSubsets = alreadyConstructedAttributeSubsets.stream()
                .map(set -> set.stream().map(name -> ArrayUtils.indexOf(columnNames, name)).collect(Collectors.toSet()))
                .collect(Collectors.toSet());
        chosenColumnIndex = chooseCorrelatedColumnIndex(histogramSize, columnPair2Frequency, alreadyConstructedIndexSubsets);

        return chosenColumnIndex.stream().map(index -> columnNames[index]).toList();
    }

    private void writeQueryWeightFile(List<SingleTableQueryRequest> singleTableQueryRequests) throws IOException {

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(QUERY_WEIGHT_FILE_NAME))) {
            StringBuilder sb = new StringBuilder();

            for (int queryId = 0; queryId < singleTableQueryRequests.size(); queryId++) {
                SingleTableQueryRequest singleTableQueryRequest = singleTableQueryRequests.get(queryId);

                sb.setLength(0); // 清空 StringBuilder
                //文件中使用的是列名在columnNames中的索引
                singleTableQueryRequest.requests().stream().forEach(req -> sb.append(ArrayUtils.indexOf(columnNames, req.columnName())).append(" "));

                sb.append(1);

                bufferedWriter.write(sb.toString());
                bufferedWriter.newLine();
            }
        }
    }

    private void writeQErrorFile(List<SingleTableQueryRequest> singleTableQueryRequests, double[] qErrors) throws IOException {
        double totalRow = Arrays.stream(qErrors).sum();
//        double scaleFactor = MAX_Q_ERROR_FILE_ROW / totalRow;
        double scaleFactor = MAX_Q_ERROR_FILE_ROW;

        // 直接使用 ColumnQueryRequest 作为 HashMap key
        Map<ColumnQueryRequest, Integer> columnIndexMap = IntStream.range(0, columnRequests.size())
                .boxed()
                .collect(Collectors.toMap(columnRequests::get, i -> i));

        int[] writeCounts = Arrays.stream(qErrors)
                .mapToInt(q -> (int) Math.round(scaleFactor * q))
                .toArray();

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(QUERY_Q_ERROR_FILE_NAME))) {
            StringBuilder sb = new StringBuilder();

            for (int queryId = 0; queryId < singleTableQueryRequests.size(); queryId++) {
                SingleTableQueryRequest singleTableQueryRequest = singleTableQueryRequests.get(queryId);

                sb.setLength(0); // 清空 StringBuilder
                singleTableQueryRequest.requests().stream()
                        .map(columnIndexMap::get) // 直接用对象引用查找索引
                        .map(String::valueOf)
                        .forEach(index -> sb.append(index).append(" "));

                sb.append(writeCounts[queryId]);

                bufferedWriter.write(sb.toString());
                bufferedWriter.newLine();
            }
        }
    }

    /**
     * 读取频繁项挖掘算法得到的频繁项集COLUMN_VALUE_FREQUENCY_ITEM_SET
     *
     * @return 频繁项集
     */
    private List<ColumnRequestAndQErrorFrequency> readFrequencyItemSet() throws IOException {
        List<ColumnRequestAndQErrorFrequency> columnRequest2Frequency = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(COLUMN_VALUE_FREQUENCY_ITEM_SET))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] columnIndexes = line.split(" ");
                // 只考虑两两谓词间频繁程度
                if (columnIndexes.length == CORRELATION_COLUMN_NUM + FREQUENCY_ITEM_SET_SIZE) {
                    List<ColumnQueryRequest> value = new ArrayList<>(CORRELATION_COLUMN_NUM);
                    for (int i = 0; i < CORRELATION_COLUMN_NUM; i++) {
                        int columnValueIndex = Integer.parseInt(columnIndexes[i]);
                        value.add(columnRequests.get(columnValueIndex));
                    }
                    int cumulativeFrequency = Integer.parseInt(columnIndexes[columnIndexes.length - 1]);
                    columnRequest2Frequency.add(new ColumnRequestAndQErrorFrequency(value, cumulativeFrequency)); // 其中value是”列+谓词+参数“的形式
                }
            }
        }
        if (logger.isEnabledForLevel(Level.DEBUG)) {
            for (var requestAndFrequency : columnRequest2Frequency.subList(0, DEBUG_OUTPUT_NUM)) {
                logger.debug("Column value frequency item set: {}, Frequency: {}",
                        requestAndFrequency.columnQueryRequests(), requestAndFrequency.qErrorFrequency());
            }
        }
        return columnRequest2Frequency;
    }

    /**
     * 读取频繁项挖掘算法得到的列队权重（即对该列对施加Frequency的查询数量，用于均一化）
     *
     * @return 列对权重
     */
    private Map<Set<Integer>, Integer> readColumnWeightFrequencyItemSet() throws IOException{
        Map<Set<Integer>, Integer> columnPairIndex2Weight = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(COLUMN_WEIGHT_FREQUENCY_ITEM_SET))){
            String line;
            while ((line = bufferedReader.readLine()) != null){
                String[] columnIndexes = line.split(" ");
                // 只考虑列对之间的权重
                if (columnIndexes.length == CORRELATION_COLUMN_NUM + FREQUENCY_ITEM_SET_SIZE){
                    Set<Integer> ColumnPairIndex = new HashSet<>(CORRELATION_COLUMN_NUM);
                    for (int i = 0; i < CORRELATION_COLUMN_NUM; i++) {
                        int columnValueIndex = Integer.parseInt(columnIndexes[i]);
                        ColumnPairIndex.add(columnValueIndex);
                    }
                    Integer weight = Integer.parseInt(columnIndexes[columnIndexes.length - 1]);
                    Integer oldValue = columnPairIndex2Weight.putIfAbsent(ColumnPairIndex, weight);
                    if (oldValue != null) {
                        columnPairIndex2Weight.put(ColumnPairIndex, oldValue + weight);
                    }
                }
            }
        }
        return columnPairIndex2Weight;
    }


    private Map<Set<Integer>, Integer> extractColumnPair2Frequency(List<ColumnRequestAndQErrorFrequency> columnValue2Frequency, Map<Set<Integer>, Integer> columnPairIndex2Weight, Set<Set<String>> alreadyConstructedAttributeSubsets) {
        Map<Set<Integer>, Integer> valueSet2Frequency = new HashMap<>();
        for (var columnValueAndFrequency : columnValue2Frequency) {
            Set<Integer> columnIndexes = new HashSet<>();
            for (var columnRequest : columnValueAndFrequency.columnQueryRequests()) {
                columnIndexes.add(ArrayUtils.indexOf(columnNames, columnRequest.columnName()));
            }
            Integer oldValue = valueSet2Frequency.putIfAbsent(columnIndexes, columnValueAndFrequency.qErrorFrequency());
            if (oldValue != null) {
                valueSet2Frequency.put(columnIndexes, oldValue + columnValueAndFrequency.qErrorFrequency());
            }
        }

        // 新增逻辑：如果该列对已经在某个直方图中被刻画，则置权重为 0
        for (Set<Integer> columnPair : valueSet2Frequency.keySet()) {
            // 将索引集合转为列名集合
            Set<String> columnNamesSet = columnPair.stream()
                    .map(index -> columnNames[index])
                    .collect(Collectors.toSet());

            // 检查该列对是否包含在已构建直方图的属性集合中
            for (Set<String> constructedSet : alreadyConstructedAttributeSubsets) {
                // 如果该 constructedSet 至少包含当前列对中的全部列，则说明这条边已被建模
                if (constructedSet.containsAll(columnNamesSet)) {
                    valueSet2Frequency.put(columnPair, 0);  // 置零
                    break;
                }
            }
        }
        
        if (logger.isDebugEnabled()){

            logger.debug("Column pair frequency item set");

            // 假设 valueSet2Frequency 是 Map<Set<Integer>, Integer> 或类似类型
            var sortedEntries = valueSet2Frequency.entrySet().stream()
                    .sorted(Map.Entry.<Set<Integer>, Integer>comparingByValue().reversed())
                    .toList();

            for (var entry : sortedEntries) {
                List<String> involvedColumns = entry.getKey().stream()
                        .map(index -> columnNames[index])
                        .toList();

                logger.debug("Column pair: {}, Frequency: {}", involvedColumns, entry.getValue());
            }
        }

        return valueSet2Frequency;
    }


    private record ColumnRequestAndQErrorFrequency(List<ColumnQueryRequest> columnQueryRequests, int qErrorFrequency) {
    }
}
