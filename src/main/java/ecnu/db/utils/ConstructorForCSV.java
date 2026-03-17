package ecnu.db.utils;

import ecnu.db.histogram.adaptor.ColumnType;
import ecnu.db.histogram.single.SingleColumnHistogram;
import ecnu.db.histogram.single.SingleColumnRealDistribution;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class ConstructorForCSV {
    private static final Pattern INT_PATTERN = Pattern.compile("[+-]?\\d+");
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ConstructorForCSV.class);
    private static int totalRow = 0;
    private final Map<String, AtomicLong> value2Count = new HashMap<>();  // 用于统计每个值出现的次数，不区分是否为高频项
    private int nullRow = 0;

    private ColumnType columnType = ColumnType.INT;

    public static int getTotalRow() {
        return totalRow;
    }

    public static ColumnHistogramBundle construct(Set<String> involvedColumns,
                                                   String csvFileName,
                                                   boolean withHeader) throws IOException {
        SequencedMap<String, SingleColumnHistogram<?>> columnHistogramMap = new LinkedHashMap<>();
        var column2Constructor = produceConstructor(csvFileName, withHeader);
        for (Map.Entry<String, ConstructorForCSV> columnConstructor : column2Constructor.entrySet()) {
            // 如果所有查询都没有涉及到某个列，则跳过
            if (!involvedColumns.contains(columnConstructor.getKey())) {
                continue;
            }
            columnHistogramMap.put(columnConstructor.getKey(), columnConstructor.getValue().makeHistogram());
        }
        return new ColumnHistogramBundle(columnHistogramMap, totalRow);
    }

    public static Map<String, SingleColumnRealDistribution<?>> constructRealDistribution(String csvFileName, boolean withHeader) throws IOException {
        Map<String, SingleColumnRealDistribution<?>> columnHistogramMap = new HashMap<>();
        var column2Constructor = produceConstructor(csvFileName, withHeader);
        for (Map.Entry<String, ConstructorForCSV> columnConstructor : column2Constructor.entrySet()) {
            ConstructorForCSV constructor = columnConstructor.getValue();
            var realDistribution = new SingleColumnRealDistribution<>(constructor.value2Count, totalRow, constructor.columnType.getColumnAdaptor());
            columnHistogramMap.put(columnConstructor.getKey(), realDistribution);
        }
        return columnHistogramMap;
    }

    private static int readFileRowCount(String filePath) throws IOException {
        int rowCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while (reader.readLine() != null) {
                rowCount++;
            }
        }
        return rowCount;
    }


    private static Map<String, ConstructorForCSV> produceConstructor(String csvFileName, boolean withHeader) throws IOException {
        Map<String, ConstructorForCSV> column2Constructor = new LinkedHashMap<>();
        String[] columnNames;
        ConstructorForCSV[] constructors;
        List<ConcurrentLinkedQueue<String>> queues = new ArrayList<>();
        totalRow = readFileRowCount(csvFileName) - (withHeader ? 1 : 0);

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFileName))) {
            String line = bufferedReader.readLine();
            String[] values = line.split(",");
            CountDownLatch countDownLatch = new CountDownLatch(values.length);
            constructors = new ConstructorForCSV[values.length];
            for (int i = 0; i < constructors.length; i++) {
                var constructor = new ConstructorForCSV();
                constructors[i] = constructor;
                ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
                queues.add(queue);
                new Thread(() -> {
                    String value;
                    for (int rowId = 0; rowId < totalRow; rowId++) {
                        while ((value = queue.poll()) == null) {
                            Thread.onSpinWait();
                        }
                        constructor.analyzeTuple(value);
                    }
                    countDownLatch.countDown();
                }).start();
            }
            if (withHeader) {
                columnNames = values;
            } else {
                columnNames = IntStream.range(0, values.length).mapToObj(String::valueOf).toArray(String[]::new);
                for (int i = 0; i < constructors.length; i++) {
                    queues.get(i).offer(values[i]);
                }
            }
            while ((line = bufferedReader.readLine()) != null) {
                int queueIndex = 0;
                for (String colValue : line.split(",")) {
                    queues.get(queueIndex++).offer(colValue);
                }
            }
            logger.info("csv file read complete, wait constructor analysis");
            //主线程调用 countDownLatch.await() 方法，它会阻塞直到 countDownLatch 的计数值变为零。
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        for (int i = 0; i < columnNames.length; i++) {
            column2Constructor.put(columnNames[i], constructors[i]);
        }
        return column2Constructor;
    }

    /**
     * 判断某个列的类型，设置这个列的columnType值。统计列中的值以及其出现次数。
     *
     * @param row 某一列的值
     */
    private void analyzeTuple(String row) {
        if (row.isEmpty()) {
            nullRow++;
        } else {
            // 统计每个值的出现次数
            value2Count.computeIfAbsent(row, k -> new AtomicLong(0L)).incrementAndGet();
            // 判断列的类型
            boolean isIntType = INT_PATTERN.matcher(row).matches();
            // 如果不能被转换为int，则设置为string
            if (!isIntType) {
                Matcher dateMather = DATE_PATTERN.matcher(row);
                boolean isDateType = dateMather.find() && dateMather.group().equals(row);
                if (isDateType) {
                    columnType = ColumnType.DATE;
                } else {
                    columnType = ColumnType.STRING;
                }
            }
        }
    }

    private SingleColumnHistogram<?> makeHistogram() {
        int ndv = value2Count.size();
        double nullProbability = (double) nullRow / totalRow;
        Map<String, Double> valueFrequency = new HashMap<>();
        for (var entry : value2Count.entrySet()) {
            valueFrequency.put(entry.getKey(), (double) entry.getValue().get() / totalRow);
        }
        return columnType.getColumnAdaptor().createFromValueFrequency(valueFrequency, ndv, nullProbability);
    }
}
