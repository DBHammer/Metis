package ecnu.db.histogram;

import java.util.Arrays;

public class QueryErrorTolerance {
    boolean[] isHistogramInvolvePartialTableOfQuery;
    boolean[] isInvolvedGeneratedHistogram;

    public QueryErrorTolerance(int length) {
        this.isHistogramInvolvePartialTableOfQuery = new boolean[length];
        this.isInvolvedGeneratedHistogram = new boolean[length];
    }

    // 设置是否容忍bayes误差
    public void setIsHistogramInvolvePartialTableOfQuery(int index, boolean value) {
        if (index >= 0 && index < isHistogramInvolvePartialTableOfQuery.length) {
            this.isHistogramInvolvePartialTableOfQuery[index] = value;
        }
    }

    // 设置是否容忍interval误差
    public void setIsInvolvedGeneratedHistogram(int index, boolean value) {
        if (index >= 0 && index < isInvolvedGeneratedHistogram.length) {
            this.isInvolvedGeneratedHistogram[index] = value;
        }
    }

    @Override
    public String toString() {
        return "QueryErrorTolerance{" +
                "isHistogramInvolvePartialTableOfQuery=" + Arrays.toString(isHistogramInvolvePartialTableOfQuery) +
                ", isInvolvedGeneratedHistogram=" + Arrays.toString(isInvolvedGeneratedHistogram) +
                '}';
    }
}
