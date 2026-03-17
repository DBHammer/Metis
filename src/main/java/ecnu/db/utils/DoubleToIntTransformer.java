package ecnu.db.utils;

public class DoubleToIntTransformer {

    private double totalDelta;

    public void addDelta(double addedDelta) {
        totalDelta += addedDelta;
    }

    /**
     * 将double转换为int, 累计误差不超过1
     *
     * @param valueNumDouble double值
     * @return int值
     */
    public int transform(double valueNumDouble) {
        int valueNum = (int) Math.round(valueNumDouble);
        double tmpDelta = valueNum - valueNumDouble + totalDelta;
        if (tmpDelta > 0.5 && valueNum > 1) {
            valueNum = (int) Math.floor(valueNumDouble);
        } else if (tmpDelta < -0.5) {
            valueNum = (int) Math.ceil(valueNumDouble);
        }
        totalDelta += valueNum - valueNumDouble;
        return valueNum;
    }
}
