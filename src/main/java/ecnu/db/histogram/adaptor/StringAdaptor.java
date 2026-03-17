package ecnu.db.histogram.adaptor;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class StringAdaptor extends ColumnTypeAdaptor<String> {
    @Override
    public String reverseToString(Object internalValue) {
        return internalValue.toString();
    }

    @Override
    public String transformByAdaptor(String value) {
        return value;
    }

    @Override
    public double computeRatioFromValueToRightBound(String leftBound, String rightBound, String value) {
        int maxLength = Math.max(Math.max(leftBound.length(), rightBound.length()), value.length());
        BigDecimal rangeSpaceDistance = computeStringDistance(leftBound, rightBound, maxLength);
        BigDecimal value2RightBoundDistance = computeStringDistance(value, rightBound, maxLength);
        return value2RightBoundDistance.divide(rangeSpaceDistance, 10, RoundingMode.HALF_UP).doubleValue();
    }

    /**
     * 生成一个随机值，该值在[leftBound, rightBound)区间内
     *
     * @param leftBound           左边界
     * @param rightBound          右边界
     * @param exclusiveRightBound 是否排除右边界
     * @return 生成的随机值
     */
    @Override
    public String generateUniformValueFromDomain(String leftBound, String rightBound, boolean exclusiveRightBound) {
        return leftBound + Character.MIN_VALUE;
    }


    /**
     * 计算两个字符串之间的逻辑距离
     *
     * @param minStr 小字符串
     * @param maxStr 大字符串
     * @param length 字符串对齐的长度
     * @return 两个字符串之间的距离
     */
    private BigDecimal computeStringDistance(String minStr, String maxStr, int length) {
        BigDecimal distance = BigDecimal.ZERO;
        BigDecimal byteSpaceSize = BigDecimal.valueOf((Character.MAX_VALUE - (Character.MIN_VALUE - 1)));
        for (int i = 0; i < length; i++) {
            int minStrByte = i < minStr.length() ? minStr.charAt(i) : Character.MIN_VALUE - 1;
            int maxStrByte = i < maxStr.length() ? maxStr.charAt(i) : Character.MIN_VALUE - 1;
            int byteDistance = maxStrByte - minStrByte;
            distance = distance.multiply(byteSpaceSize).add(BigDecimal.valueOf(byteDistance));
        }
        return distance;
    }

    @Override
    public boolean isNeighbour(String leftValue, String rightValue) {
        return false;
    }

    @Override
    public double ratioInRightClosedInterval(
            String left, String right, String value) {
        /**
         * 字符串类型没有天然的离散步长或前驱（predecessor）定义，
         * 无法严格区分 (left, right] 与 (left, right) 两种区间语义。
         *
         * 在本系统中，字符串列的连续区间比例是基于字典序排序空间的
         * 距离近似来计算的，该近似在统计意义上对右端点是否闭合不敏感。
         *
         * 因此，对于字符串类型，右闭区间 (left, right] 的比例计算
         * 直接复用原有的排序空间距离逻辑。
         */
        return computeRatioFromValueToRightBound(left, right, value);
    }

    @Override
    public double ratioInRightOpenInterval(
            String left, String right, String value) {
        /**
         * 对字符串类型而言，高频值仅作为概率切断点（MCV），
         * 而不会像整数或日期类型那样缩短连续值域的物理长度。
         *
         * 因此，在 rightBound 为高频项的情况下，字符串列
         * 不区分右开区间 (left, right) 与右闭区间 (left, right]，
         * 其连续区间比例在统计近似上视为等价。
         *
         * 为保持区间语义的一致性与实现的稳定性，
         * 此处直接复用右闭区间的比例计算逻辑。
         */
        return computeRatioFromValueToRightBound(left, right, value);
    }
}
