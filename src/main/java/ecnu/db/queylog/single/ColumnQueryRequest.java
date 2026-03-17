package ecnu.db.queylog.single;

import ecnu.db.histogram.RangeType;

/**
 * 来自查询的请求
 *
 * @param columnName 查询涉及到的列
 * @param rangeType  该查询的类型
 * @param value      查询的参数值
 */
public record ColumnQueryRequest(String columnName, RangeType rangeType, String value) {
    @Override
    public String toString() {
        return columnName + " " + rangeType + " '" + value + "'";
    }
}