package ecnu.db.utils;

import ecnu.db.histogram.single.SingleColumnHistogram;

import java.io.Serializable;
import java.util.Map;
import java.util.SequencedMap;

public class ColumnHistogramBundle implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SequencedMap<String, SingleColumnHistogram<?>> histogramMap;
    private final int totalRow;

    public ColumnHistogramBundle(SequencedMap<String, SingleColumnHistogram<?>> histogramMap, int totalRow) {
        this.histogramMap = histogramMap;
        this.totalRow = totalRow;
    }

    public SequencedMap<String, SingleColumnHistogram<?>> getHistogramMap() {
        return histogramMap;
    }

    public int getTotalRow() {
        return totalRow;
    }
}

