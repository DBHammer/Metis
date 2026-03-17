package ecnu.db.histogram.single;

import ecnu.db.histogram.RangeType;
import ecnu.db.histogram.adaptor.ColumnTypeAdaptor;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class SingleColumnRealDistribution<T extends Comparable<T> & Serializable> {
    private final NavigableMap<T, Double> value2Probability;

    private final ColumnTypeAdaptor<T> columnTypeAdaptor;

    public SingleColumnRealDistribution(Map<String, AtomicLong> value2Count, int totalRow,
                                        ColumnTypeAdaptor<T> columnTypeAdaptor) {
        this.value2Probability = new TreeMap<>();
        this.columnTypeAdaptor = columnTypeAdaptor;
        for (Map.Entry<String, AtomicLong> valueAndCount : value2Count.entrySet()) {
            value2Probability.put(columnTypeAdaptor.transform(valueAndCount.getKey()),
                    (double) valueAndCount.getValue().get() / totalRow);
        }
    }

    public double getProbability(String inputValue, RangeType rangeType) {
        T value = columnTypeAdaptor.transform(inputValue);
        return switch (rangeType) {
            case EQ -> value2Probability.get(value);
            case GREATER_EQ ->
                    value2Probability.tailMap(value, true).values().stream().mapToDouble(Double::doubleValue).sum();
            case LESS_EQ ->
                    value2Probability.headMap(value, true).values().stream().mapToDouble(Double::doubleValue).sum();
        };
    }
}
