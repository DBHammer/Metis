package ecnu.db.histogram;

/**
 * 用于构造多列直方图的请求
 *
 * @param rangeIndex        请求开始的range index
 * @param currentRangeRatio 该range被占用的比例
 * @param rangeType         该查询的类型
 */
public record HistogramRequest(int rangeIndex, double currentRangeRatio, RangeType rangeType) {
}
