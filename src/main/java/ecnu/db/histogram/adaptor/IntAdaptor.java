package ecnu.db.histogram.adaptor;

import java.util.concurrent.ThreadLocalRandom;

public class IntAdaptor extends ColumnTypeAdaptor<Integer> {

    @Override
    public String reverseToString(Object internalValue) {
        return internalValue.toString();
    }

    @Override
    public Integer transformByAdaptor(String value) {
        return Integer.valueOf(value);
    }

    @Override
    public double computeRatioFromValueToRightBound(Integer leftBound, Integer rightBound, Integer value) {
        return (double) (rightBound - value) / (rightBound - leftBound);
    }

    @Override
    public Integer generateUniformValueFromDomain(Integer leftBound, Integer rightBound, boolean exclusiveRightBound) {
        return ThreadLocalRandom.current().nextInt(leftBound, rightBound + (exclusiveRightBound ? 0 : 1));
    }

    @Override
    public boolean isNeighbour(Integer leftValue, Integer rightValue) {
        return leftValue + 1 == rightValue;
    }

    @Override
    public double ratioInRightClosedInterval(
            Integer left, Integer right, Integer value) {

        return (double) (right - value) / (right - left);
    }

    @Override
    public double ratioInRightOpenInterval(
            Integer left, Integer right, Integer value) {

        // (left, right) 等价于 (left, right-1]，前提 right > left+1
        int effectiveRight = right - 1;

        return (double) (effectiveRight - value) / (effectiveRight - left);
    }
}
