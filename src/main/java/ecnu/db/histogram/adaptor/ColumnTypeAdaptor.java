package ecnu.db.histogram.adaptor;

import ecnu.db.histogram.single.SingleColumnHistogram;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class ColumnTypeAdaptor<T extends Comparable<T> & Serializable>  implements Serializable{
    private static final long serialVersionUID = 1L;
    private static final int MAX_HIGH_FREQUENCY_ITEM = 100;
    private static final int RANGE_OF_HISTOGRAM = 100;
    private final ConcurrentHashMap<String, T> valueMap = new ConcurrentHashMap<>();

    public T transform(String value) {
        return valueMap.computeIfAbsent(value, k -> transformByAdaptor(value));
    }

    public int compare(String value1, String value2) {
        T transformedValue1 = transform(value1);
        T transformedValue2 = transform(value2);
        return transformedValue1.compareTo(transformedValue2);
    }

    public abstract String reverseToString(Object internalValue);

    public abstract T transformByAdaptor(String value);

    public abstract double computeRatioFromValueToRightBound(T leftBound, T rightBound, T value);

    public abstract T generateUniformValueFromDomain(T leftBound, T rightBound, boolean exclusiveRightBound);

    public abstract boolean isNeighbour(T leftValue, T rightValue);

    /**
     * 从valueFrequency中创建直方图
     *
     * @param valueFrequency  每个value对应的概率
     * @param ndv             该列的ndv
     * @param nullProbability 该列的null概率
     * @return 该列的直方图
     */
    public SingleColumnHistogram<T> createFromValueFrequency(Map<String, Double> valueFrequency, int ndv,
                                                             double nullProbability) {
        NavigableMap<T, Double> highFrequencyItem = new TreeMap<>();
        SortedMap<T, Double> normalItem = new TreeMap<>();
        PriorityQueue<T> histogram = new PriorityQueue<>();

        if (valueFrequency.size() < MAX_HIGH_FREQUENCY_ITEM + RANGE_OF_HISTOGRAM - 1) {
            for (Map.Entry<String, Double> entry : valueFrequency.entrySet()) {
                highFrequencyItem.put(transform(entry.getKey()), entry.getValue());
            }
            return new SingleColumnHistogram<>(highFrequencyItem, histogram, ndv, nullProbability, this);
        }

        // 1. 找到最小的键
        T minKey = valueFrequency.keySet().stream()
                .map(this::transform)
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalStateException("valueFrequency 为空"));

        // 2. 计算高频项的阈值
        int splitCountIndex = Math.max(valueFrequency.size() - 1 - MAX_HIGH_FREQUENCY_ITEM, 0);
        double smallestHighFrequencyItemProbability = valueFrequency.values().stream()
                .sorted()
                .toList()
                .get(splitCountIndex);

        // 3. 分配 highFrequencyItem 和 normalItem，并确保 `minKey` 在 `highFrequencyItem`
        for (Map.Entry<String, Double> entry : valueFrequency.entrySet()) {
            T key = transform(entry.getKey());
            if (key.equals(minKey) || entry.getValue() > smallestHighFrequencyItemProbability) {
                highFrequencyItem.put(key, entry.getValue());
            } else {
                normalItem.put(key, entry.getValue());
            }
        }
        // 计算平均概率
        double avgFrequency = normalItem.values().stream().mapToDouble(Double::doubleValue).sum() / RANGE_OF_HISTOGRAM;

        // 4. 计算直方图桶的边界
        if (!normalItem.isEmpty()) {
            histogram.add(normalItem.firstKey());
            var mapIterator = normalItem.entrySet().iterator();
            var currentEntry = mapIterator.next();
            double cumulativeFrequency = currentEntry.getValue();
            for (int i = 1; i <= RANGE_OF_HISTOGRAM; i++) {
                // 找到下一个直方图的边界
                while (cumulativeFrequency < i * avgFrequency) {
                    if (mapIterator.hasNext()) {
                        currentEntry = mapIterator.next();
                    }
                    cumulativeFrequency += currentEntry.getValue();
                }
                histogram.add(currentEntry.getKey());
            }
        }
        return new SingleColumnHistogram<>(highFrequencyItem, histogram, ndv, nullProbability, this);
    }

    /**
     * value 在 (left, right] 中的比例
     */
    public abstract double ratioInRightClosedInterval(
            T left, T right, T value);

    /**
     * value 在 (left, right) 中的比例
     */
    public abstract double ratioInRightOpenInterval(
            T left, T right, T value);
}
