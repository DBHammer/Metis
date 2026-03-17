package ecnu.db.histogram;

/**
 * 记录单列上桶索引，以及桶覆盖率
 * @param index
 * @param ratio
 */
public record BucketOfSingleColumn(int index, double ratio) {
}
