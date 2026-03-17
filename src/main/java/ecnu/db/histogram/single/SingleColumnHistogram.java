package ecnu.db.histogram.single;

import ecnu.db.histogram.BucketOfSingleColumn;
import ecnu.db.histogram.HistogramRequest;
import ecnu.db.histogram.RangeType;
import ecnu.db.histogram.adaptor.ColumnTypeAdaptor;
import ecnu.db.histogram.adaptor.StringAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class UsedBucket {
    final int index;
    final double ratio;

    UsedBucket(int index, double ratio) {
        this.index = index;
        this.ratio = ratio;
    }
}


public class SingleColumnHistogram<T extends Comparable<T> & Serializable> implements Serializable {

    private static class Segment<T> {
        final T left;
        final T right;
        final boolean rightOpen; // true: (l, r) ; false: (l, r]

        Segment(T left, T right, boolean rightOpen) {
            this.left = left;
            this.right = right;
            this.rightOpen = rightOpen;
        }
    }

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(SingleColumnHistogram.class);
    /**
     * 记录每个区间的概率，从左边界到右边界，前开后闭。
     * 对于高频项目，对应两个概率值第一个值为左边界到右边界，前开后开，第二个值为等值概率
     * 补充：rangeProbability的最后一项为null的概率，不存在null项时，其概率为0
     */
    private final double[] rangeProbability;
    /**
     * 底层数据的 number of distinct value，由分析底层数据获得
     */
    private final int ndv;
    /**
     * 除高频项之外的数据项的平均概率
     */
    private final double avgProbabilityForNormalFrequencyItem;
    /**
     * 直方图的整个domain空间 -> rangeProbability的索引位置
     * 第一个值为该列最小的值，后续的每个值为等频直方图的右边界，前开后闭
     * 每种数据类型的实现都需要将其空间映射到整数空间中
     * 例如：int类型的直方图，domain空间为[1, 2, 3, 4, 5]，则映射为[0, 1, 2, 3, 4]
     * 对于高频项，包含两个映射位点, 后一位表示其概率
     * 例如：int类型的直方图，domain空间为[1, 2, 3, 4, 5]，高频项为[1, 2]，则映射为[1+1, 2+1, 4, 5, 6]
     */
    private final NavigableMap<T, Integer> domain = new TreeMap<>();
    /**
     * 高频项的domain空间
     */
    private final SortedSet<T> highFrequencyItems = new TreeSet<>();
    private final ColumnTypeAdaptor<T> columnTypeAdaptor;
    private final double eachRangeProbability;

    /**
     * 适配范型T的直方图
     *
     * @param highFrequencyItem2Probability 高频项及其概率
     * @param histogramIgnoreHighFrequency  第一个值为该列最小的值，后续的每个值为等频直方图的右边界，前开后闭。可能存在重复值。
     * @param ndv                           列内非重复值的个数
     * @param nullProbability               该列的null比例
     */
    public SingleColumnHistogram(NavigableMap<T, Double> highFrequencyItem2Probability,
                                 PriorityQueue<T> histogramIgnoreHighFrequency,
                                 int ndv, double nullProbability, ColumnTypeAdaptor<T> columnTypeAdaptor) {
        this.ndv = ndv;
        this.columnTypeAdaptor = columnTypeAdaptor;
        if (histogramIgnoreHighFrequency.stream().anyMatch(highFrequencyItem2Probability::containsKey)) {
            throw new IllegalStateException("高频项和非高频项存在重复");
        }
        // 计算所有高频项的累计概率
        double totalProbabilityForHighFrequencyItem = highFrequencyItem2Probability.values().stream()
                .mapToDouble(Double::doubleValue).sum();
        // 计算有效的概率空间
        double validProbability = 1 - nullProbability - totalProbabilityForHighFrequencyItem;
        // 计算每个range的概率大小  eachRangeProbability = ColumnTypeAdaptor的createFromValueFrequency函数中的avgFrequency
        eachRangeProbability = validProbability / (histogramIgnoreHighFrequency.size() - 1);
        // 计算非高频项的平均概率
        int remainSize = ndv - highFrequencyItem2Probability.size();
        avgProbabilityForNormalFrequencyItem = remainSize > 0 ? validProbability / remainSize : 0;
        // 如果直方图中有重复值，则把它直接当作高频项，概率为出现的次数*eachRangeProbability，然后将其从直方图中移除
        dealWithDuplicateValueInHistogram(highFrequencyItem2Probability, histogramIgnoreHighFrequency, eachRangeProbability);
        // 初始化domain空间到int空间的映射
        int highFrequencyDelta = histogramIgnoreHighFrequency.isEmpty() ? 0 : 1;
        rangeProbability = new double[histogramIgnoreHighFrequency.size() + (1 + highFrequencyDelta) * highFrequencyItem2Probability.size() + 1];
        Arrays.fill(rangeProbability, 0);
        rangeProbability[rangeProbability.length - 1] = nullProbability;
        initDomain2IndexMapping(highFrequencyItem2Probability, histogramIgnoreHighFrequency, highFrequencyDelta);
        // 如果该列只有高频项，则直接构建单列直方图即可。当除了高频项外，仍有直方图区间时，则会进入if中。
        if (!histogramIgnoreHighFrequency.isEmpty()) {
            // 将直方图中的值合并到rangeProbability中
            mergeHistogram(histogramIgnoreHighFrequency, eachRangeProbability);
        }
        double deltaProbability = 1 - Arrays.stream(rangeProbability).sum();
        // 精度误差 1e-15是最小的精度上线
        if (deltaProbability != 0 && Math.abs(deltaProbability) <= 1e-15) {
            for (int i = 0; i < rangeProbability.length; i++) {
                // 把精度损失填回某一个range中
                if (rangeProbability[i] != 0) {
                    rangeProbability[i] += deltaProbability;
                    deltaProbability = 0;
                    break;
                }
            }
        }
        if (deltaProbability != 0) {
            throw new IllegalStateException("rangeProbability的概率和不为1");
        }
    }

    public double getEachRangeProbability() {
        return eachRangeProbability;
    }

    /**
     * 处理直方图中的重复值
     *
     * @param highFrequencyItem2Probability 高频项及其概率
     * @param histogramIgnoreHighFrequency  直方图中的值
     * @param eachRangeProbability          每个range的概率
     */
    private void dealWithDuplicateValueInHistogram(NavigableMap<T, Double> highFrequencyItem2Probability,
                                                   PriorityQueue<T> histogramIgnoreHighFrequency,
                                                   double eachRangeProbability) {
        SortedMap<T, Integer> histogramIgnoreHighFrequency2RangeNum = new TreeMap<>();
        for (T item : histogramIgnoreHighFrequency) {
            histogramIgnoreHighFrequency2RangeNum.putIfAbsent(item, 0);
            histogramIgnoreHighFrequency2RangeNum.put(item, histogramIgnoreHighFrequency2RangeNum.get(item) + 1);
        }
        for (Map.Entry<T, Integer> histogram2RangeNum : histogramIgnoreHighFrequency2RangeNum.entrySet()) {
            // 对于重复出现在直方图中的值，如果其出现次数大于1，则将其视为高频项
            // 例如：直方图为[1, 2, 3, 3, 4, 5]，则将3视为高频项
            if (histogram2RangeNum.getValue() > 1) {
                // 计算该值的概率，将其放入高频项，然后将其从直方图中移除
                double probability = eachRangeProbability * histogram2RangeNum.getValue();
                histogramIgnoreHighFrequency.removeIf(v -> v.equals(histogram2RangeNum.getKey()));
                // 对于最后一个高频项目，移除的range的数量为值的数量减1
                if (histogramIgnoreHighFrequency.isEmpty()) {
                    // n个值对应(n-1)个概率区间，所以需要舍去一个值的概率区间
                    probability -= eachRangeProbability;
                }
                highFrequencyItem2Probability.put(histogram2RangeNum.getKey(), probability);
            }
        }
        // 如果只有一个项了，则构建了一个全高频项直方图，直接删掉该值即可
        if (histogramIgnoreHighFrequency.size() == 1) {
            histogramIgnoreHighFrequency.poll();
        }
    }

    private void initDomain2IndexMapping(NavigableMap<T, Double> highFrequencyItem2Probability,
                                         PriorityQueue<T> histogramIgnoreHighFrequency, int highFrequencyDelta) {
        // 记录高频项的domain空间
        this.highFrequencyItems.addAll(highFrequencyItem2Probability.keySet());
        // 维护直方图domain到int的映射
        for (T item : highFrequencyItem2Probability.keySet()) {
            domain.put(item, 0);
        }
        for (T item : histogramIgnoreHighFrequency) {
            domain.put(item, 0);
        }
        int index = 0;
        for (Map.Entry<T, Integer> item2Index : domain.entrySet()) {
            // 如果为高频项，则将其概率记录到domain index的下一个位点
            if (highFrequencyItems.contains(item2Index.getKey())) {
                index += highFrequencyDelta;
                rangeProbability[index] = highFrequencyItem2Probability.get(item2Index.getKey());
            }
            item2Index.setValue(index);
            index++;
        }
    }

    /**
     * 将高频项merge到直方图中（即，填充rangeProbability中，高频项对应的左开右开区间概率，和，range中右侧剩余的左开右闭概率）
     * 注意，这个函数只merge存在于histogramIgnoreHighFrequency之间的高频项。头和尾的高频项仍只有等频概率。
     *
     * @param histogramIgnoreHighFrequency 高频项
     * @param eachRangeProbability         每个range的概率
     */
//    private void mergeHistogram(PriorityQueue<T> histogramIgnoreHighFrequency, double eachRangeProbability) {
//        double lastRangeRatio = 0;
//        Map<Integer, Double> rangeIndex2RangeRatio = new HashMap<>();
//        // 忽略掉直方图第一项，默认设置其概率为0。PS：这样只会导致一个对于第一个值的等值估计始终为0。
//        T currentRangeLeftBound = histogramIgnoreHighFrequency.poll();
//        T currentRangeRightBound = histogramIgnoreHighFrequency.poll();
//        assert currentRangeRightBound != null;
//        var validDomain = domain.tailMap(currentRangeLeftBound, false);
//        for (Map.Entry<T, Integer> value2Index : validDomain.entrySet()) {
//            T value = value2Index.getKey();
//            // 如果该值超过了右边界，则推进到下一个range。 currentRangeRightBound 小于 value 时，条件成立
//            if (currentRangeRightBound.compareTo(value) < 0) {
//                logger.error(
//                        "[MERGE] switch range: L={} -> R={}",
//                        currentRangeLeftBound,
//                        currentRangeRightBound
//                );
//                currentRangeLeftBound = currentRangeRightBound;
//                // domain从现在开始都是高频项，因此直接退出即可
//                if (histogramIgnoreHighFrequency.isEmpty()) {
//                    break;
//                }
//                currentRangeRightBound = histogramIgnoreHighFrequency.poll();
//            }
//            // 计算从左边界到高频值的概率
////            double ratioToRight = columnTypeAdaptor.computeRatioFromValueToRightBound(currentRangeLeftBound, currentRangeRightBound, value);
////            double ratioToLeft = 1 - ratioToRight;
//            T rightKey = currentRangeRightBound;
//            boolean rightIsMCV = highFrequencyItems.contains(rightKey);
//
//            double ratioToRight;
//            if (rightIsMCV && !(columnTypeAdaptor instanceof StringAdaptor)) {
//                // 右边界是 MCV，普通区间必须是右开 (L, R)
//                ratioToRight =
//                        columnTypeAdaptor.ratioInRightOpenInterval(
//                                currentRangeLeftBound, rightKey, value);
//            } else {
//                // 右边界不是 MCV，普通区间可以右闭 (L, R]
//                ratioToRight =
//                        columnTypeAdaptor.computeRatioFromValueToRightBound(
//                                currentRangeLeftBound, rightKey, value);
//            }
//
//            double ratioToLeft = 1 - ratioToRight;
//            // 如果高频项和前一项是邻居，那就意味着这个range不存在任何ratio
//            boolean isHighFrequencyItem = highFrequencyItems.contains(value);
//            boolean isEndOfNormalRange =
//                    !isHighFrequencyItem &&
//                            (
//                                    (!rightIsMCV && currentRangeRightBound.equals(value))
//                                            || (rightIsMCV && columnTypeAdaptor.isNeighbour(value, currentRangeRightBound))
//                            );
//
//
//            // domain 里的前一个真实值
//            T prevValue = domain.lowerKey(value);
//
//            // 是否为连续的两个 MCV
//            boolean consecutiveMCVs =
//                    isHighFrequencyItem
//                            && prevValue != null
//                            && highFrequencyItems.contains(prevValue)
//                            && columnTypeAdaptor.isNeighbour(prevValue, value);
//
//            // 是否左边界紧邻
//            boolean neighbourToLeftBound =
//                    columnTypeAdaptor.isNeighbour(currentRangeLeftBound, value);
//
//            // 区间在值域上是否为空
//            boolean withoutValueInTheRange =
//                    isHighFrequencyItem && (consecutiveMCVs || neighbourToLeftBound);
//
//            double rangeRatio =
//                    withoutValueInTheRange ? 0.0 : ratioToLeft - lastRangeRatio;
//            logger.error(
//                    "[VAL] value={} idx={} | L={} R={} | rightIsMCV={} isMCV={} isEnd={} "
//                            + "| ratioToLeft={} last={} delta={}",
//                    value,
//                    value2Index.getValue(),
//                    currentRangeLeftBound,
//                    currentRangeRightBound,
//                    rightIsMCV,
//                    isHighFrequencyItem,
//                    isEndOfNormalRange,
//                    ratioToLeft,
//                    lastRangeRatio,
//                    rangeRatio
//            );
//            lastRangeRatio = ratioToLeft;
//            // 如果当前的值为高频项，并记录前一项range的Ratio
//            if (isHighFrequencyItem) {
//                rangeIndex2RangeRatio.put(value2Index.getValue() - 1, rangeRatio);
//            }
//            // 如果当前的值和右边界相等，则说明该值为普通项，则根据ratio更新概率
//            else if (isEndOfNormalRange) {
//                rangeIndex2RangeRatio.put(value2Index.getValue(), rangeRatio);
//                double sumRatio = rangeIndex2RangeRatio.values().stream().mapToDouble(Double::doubleValue).sum();
//                rangeIndex2RangeRatio.forEach((rangeId, ratio) -> rangeProbability[rangeId] = ratio / sumRatio * eachRangeProbability);
//                rangeIndex2RangeRatio.clear();
//                lastRangeRatio = 0;
//            } else {
//                throw new IllegalStateException();
//            }
//        }
//    }

    private void mergeHistogram(
            PriorityQueue<T> histogramIgnoreHighFrequency,
            double eachRangeProbability) {

        if (columnTypeAdaptor instanceof StringAdaptor) {
            // 字符串：继续用原来的 ratio-based merge
            mergeHistogramByRatio(histogramIgnoreHighFrequency, eachRangeProbability);
        } else {
            // 离散类型：用新的 segment + length merge
            mergeHistogramByDiscreteLength(histogramIgnoreHighFrequency, eachRangeProbability);
        }
    }

    private void mergeHistogramByRatio(PriorityQueue<T> histogramIgnoreHighFrequency, double eachRangeProbability) {
        double lastRangeRatio = 0;
        Map<Integer, Double> rangeIndex2RangeRatio = new HashMap<>();
        // 忽略掉直方图第一项，默认设置其概率为0。PS：这样只会导致一个对于第一个值的等值估计始终为0。
        T currentRangeLeftBound = histogramIgnoreHighFrequency.poll();
        T currentRangeRightBound = histogramIgnoreHighFrequency.poll();
        assert currentRangeRightBound != null;
        var validDomain = domain.tailMap(currentRangeLeftBound, false);
        for (Map.Entry<T, Integer> value2Index : validDomain.entrySet()) {
            T value = value2Index.getKey();
            // 如果该值超过了右边界，则推进到下一个range。 currentRangeRightBound 小于 value 时，条件成立
            if (currentRangeRightBound.compareTo(value) < 0) {
                logger.error(
                        "[MERGE] switch range: L={} -> R={}",
                        currentRangeLeftBound,
                        currentRangeRightBound
                );
                currentRangeLeftBound = currentRangeRightBound;
                // domain从现在开始都是高频项，因此直接退出即可
                if (histogramIgnoreHighFrequency.isEmpty()) {
                    break;
                }
                currentRangeRightBound = histogramIgnoreHighFrequency.poll();
            }
            // 计算从左边界到高频值的概率
            double ratioToRight = columnTypeAdaptor.computeRatioFromValueToRightBound(currentRangeLeftBound, currentRangeRightBound, value);
            double ratioToLeft = 1 - ratioToRight;
            // 如果高频项和前一项是邻居，那就意味着这个range不存在任何ratio
            boolean isHighFrequencyItem = highFrequencyItems.contains(value);

            // domain 里的前一个真实值
            T prevValue = domain.lowerKey(value);

            // 是否为连续的两个 MCV
            boolean consecutiveMCVs =
                    isHighFrequencyItem
                            && prevValue != null
                            && highFrequencyItems.contains(prevValue)
                            && columnTypeAdaptor.isNeighbour(prevValue, value);

            // 是否左边界紧邻
            boolean neighbourToLeftBound =
                    columnTypeAdaptor.isNeighbour(currentRangeLeftBound, value);

            // 区间在值域上是否为空
            boolean withoutValueInTheRange =
                    isHighFrequencyItem && (consecutiveMCVs || neighbourToLeftBound);

            double rangeRatio =
                    withoutValueInTheRange ? 0.0 : ratioToLeft - lastRangeRatio;
            logger.error(
                    "[VAL] value={} idx={} | L={} R={} | isMCV={}  "
                            + "| ratioToLeft={} last={} delta={}",
                    value,
                    value2Index.getValue(),
                    currentRangeLeftBound,
                    currentRangeRightBound,
                    isHighFrequencyItem,
                    ratioToLeft,
                    lastRangeRatio,
                    rangeRatio
            );
            lastRangeRatio = ratioToLeft;
            // 如果当前的值为高频项，并记录前一项range的Ratio
            if (isHighFrequencyItem) {
                rangeIndex2RangeRatio.put(value2Index.getValue() - 1, rangeRatio);
            }
            // 如果当前的值和右边界相等，则说明该值为普通项，则根据ratio更新概率
            else if (currentRangeRightBound.equals(value)) {
                rangeIndex2RangeRatio.put(value2Index.getValue(), rangeRatio);
                double sumRatio = rangeIndex2RangeRatio.values().stream().mapToDouble(Double::doubleValue).sum();
                rangeIndex2RangeRatio.forEach((rangeId, ratio) -> rangeProbability[rangeId] = ratio / sumRatio * eachRangeProbability);
                rangeIndex2RangeRatio.clear();
                lastRangeRatio = 0;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private void mergeHistogramByDiscreteLength(
            PriorityQueue<T> histogramIgnoreHighFrequency,
            double eachRangeProbability) {

        // 至少需要两个点才能形成一个等频区间
        T left = histogramIgnoreHighFrequency.poll();
        if (left == null) {
            return;
        }

        while (!histogramIgnoreHighFrequency.isEmpty()) {

            T right = histogramIgnoreHighFrequency.poll();

            // (left, right] 内的 domain 值（包含 MCV）
            NavigableMap<T, Integer> subDomain =
                    domain.subMap(left, false, right, true);

            /* =====================================================
             * 1. 用 MCV 切割普通区间，构造 segments
             * ===================================================== */
            List<Segment<T>> segments = new ArrayList<>();

            T segLeft = left;

            for (T v : subDomain.keySet()) {

                if (!highFrequencyItems.contains(v)) {
                    continue;
                }

                boolean leftIsMCV = highFrequencyItems.contains(segLeft);
                boolean adjacentMCV =
                        leftIsMCV
                                && columnTypeAdaptor.isNeighbour(segLeft, v);

                // (segLeft, v) 只有在“非相邻 MCV”时才是有效普通段
                if (!adjacentMCV) {
                    segments.add(new Segment<>(segLeft, v, true)); // (l, v)
                }

                // v 作为新的切割点
                segLeft = v;
            }

            // 最后一个普通段 (segLeft, right]
            segments.add(new Segment<>(segLeft, right, false));

            /* =====================================================
             * 2. 计算每个 segment 的离散长度
             * ===================================================== */
            long totalLen = 0;
            Map<Segment<T>, Long> seg2len = new HashMap<>();

            for (Segment<T> seg : segments) {
                long len = computeDiscreteLength(seg.left, seg.right, seg.rightOpen);
                if (len > 0) {
                    seg2len.put(seg, len);
                    totalLen += len;
                }
            }

            // 如果整个区间没有普通值，直接跳过
            if (totalLen == 0) {
                left = right;
                continue;
            }

            /* =====================================================
             * 3. 按离散长度比例分配 eachRangeProbability
             * ===================================================== */
            for (Map.Entry<Segment<T>, Long> e : seg2len.entrySet()) {

                Segment<T> seg = e.getKey();
                long len = e.getValue();

                double prob = (double) len / totalLen * eachRangeProbability;

                // 写入“普通区间 bucket”
                int bucketIdx = domain.get(seg.right);

                // 如果右端点是 MCV，则普通区间 bucket 在其左侧
                if (highFrequencyItems.contains(seg.right)) {
                    bucketIdx--;
                }

                rangeProbability[bucketIdx] += prob;
            }

            // 推进到下一个等频区间
            left = right;
        }
    }


    @SuppressWarnings("unchecked")
    private long computeDiscreteLength(T left, T right, boolean rightOpen) {
        if (left == null || right == null) {
            return 0;
        }

        // 目前你已确认：只处理离散类型
        if (left instanceof Integer l && right instanceof Integer r) {
            if (rightOpen) {
                // (l, r) -> r - l - 1
                return Math.max(0, (long) r - l - 1);
            } else {
                // (l, r] -> r - l
                return Math.max(0, (long) r - l);
            }
        }

        throw new IllegalStateException(
                "Discrete length not supported for type: " + left.getClass());
    }

    /**
     * 返回查询的概率
     *
     * @param inputValue 待查询的值
     * @param rangeType  查询的类型
     * @return 满足该条件的概率
     */
    public double getProbability(String inputValue, RangeType rangeType) {
        HistogramRequest histogramRequest = getHistogramRequest(inputValue, rangeType);
        return getProbability(histogramRequest);
    }

    public double getProbability(HistogramRequest req) {

        int idx = req.rangeIndex();
        int last = rangeProbability.length - 1;
//        List<UsedBucket> usedBuckets = new ArrayList<>();

        // =====================================================
        // 1. domain 外谓词：根据 rangeType 短路返回
        // =====================================================

        // value < min（你用 idx = -1 编码）
        if (idx < 0) {
            return switch (req.rangeType()) {
                case LESS_EQ -> 0.0;
                case GREATER_EQ -> 1.0;
                case EQ -> 0.0;
            };
        }

        // value > max（你用 idx = last 且 currentRangeRatio = 0 编码）
        // 说明：domain 内命中最后一个 bucket 时，currentRangeRatio 通常不会恒为 0
        // 因而这个条件可用于区分“右越界”与“落在最后 bucket 内”
        if (idx == last && req.currentRangeRatio() == 0.0) {
            return switch (req.rangeType()) {
                case LESS_EQ -> 1.0;
                case GREATER_EQ -> 0.0;
                case EQ -> 0.0;
            };
        }

        // =====================================================
        // 2. 当前 bucket 的部分覆盖概率
        // =====================================================

        double prob = 0.0;
        if (req.currentRangeRatio() > 0) {
//            usedBuckets.add(new UsedBucket(idx, req.currentRangeRatio()));
            prob += req.currentRangeRatio() * rangeProbability[idx];
        }

        // =====================================================
        // 3. 完整 bucket 累加（防御性边界处理，避免 limit(-1)）
        // =====================================================

        switch (req.rangeType()) {

            case GREATER_EQ -> {
                for (int i = idx + 1; i < last; i++) {
//                    usedBuckets.add(new UsedBucket(i, 1.0));
                    prob += rangeProbability[i];
                }
            }

            case LESS_EQ -> {
                for (int i = 0; i < idx; i++) {
//                    usedBuckets.add(new UsedBucket(i, 1.0));
                    prob += rangeProbability[i];
                }
            }

            case EQ -> {
                // nothing
            }
        }

//        logger.debug(
//                "req={}, usedBuckets={}, prob={}",
//                req,
//                usedBuckets.stream()
//                        .map(b -> String.format("(idx=%d, ratio=%.4f)", b.index, b.ratio))
//                        .toList(),
//                prob
//        );

        return prob;
    }

    /**
     *返回当前列的所有桶
     */
    public List<BucketOfSingleColumn> getSingleColumnBucketsOfWholeDomain() {
        return IntStream.range(0, rangeProbability.length)
                .parallel()
                .mapToObj(index -> new BucketOfSingleColumn(index, 1))
                .collect(Collectors.toList());
    }

    private List<BucketOfSingleColumn> allBucketsWithRatio1() {
        int last = rangeProbability.length - 1; // 最后一位是 null bucket
        List<BucketOfSingleColumn> result = new ArrayList<>(last);

        for (int i = 0; i < last; i++) {
            // 只返回真实存在概率质量的 bucket
            if (rangeProbability[i] != 0.0) {
                result.add(new BucketOfSingleColumn(i, 1.0));
            }
        }

        return result;
    }

    /**
     * 根据直方图请求返回桶
     */
    public List<BucketOfSingleColumn> getSingleColumnBucketsFromRequest(String inputValue, RangeType rangeType) {
        T value = columnTypeAdaptor.transform(inputValue);

        if (value.compareTo(domain.firstKey()) < 0) {
            if (rangeType == RangeType.LESS_EQ) {
                return Collections.emptyList(); // <= min-1 → 0
            } else if (rangeType == RangeType.GREATER_EQ) {
                return allBucketsWithRatio1();  // >= min → 1
            } else { // EQ
                return Collections.emptyList();
            }
        }

        if (value.compareTo(domain.lastKey()) > 0) {
            if (rangeType == RangeType.GREATER_EQ) {
                return Collections.emptyList(); // >= max+1 → 0
            } else if (rangeType == RangeType.LESS_EQ) {
                return allBucketsWithRatio1();  // <= max → 1
            } else {
                return Collections.emptyList();
            }
        }


        Map.Entry<T, Integer> rightBound = domain.ceilingEntry(value);
        int rightBoundIndex = rightBound.getValue();
        T rightKey = rightBound.getKey();
        T leftKey = domain.lowerKey(value);

        double rightRatio = 0.0;
        if (leftKey != null) {
            rightRatio = columnTypeAdaptor.computeRatioFromValueToRightBound(leftKey, rightKey, value);
        }

        double eqRatio = 1.0;
        if (!highFrequencyItems.contains(value)) {
            boolean isRightBoundHigh = highFrequencyItems.contains(rightKey);
            if (isRightBoundHigh) {
                rightBoundIndex--;
            }

            if (columnTypeAdaptor instanceof StringAdaptor) {
                eqRatio = avgProbabilityForNormalFrequencyItem / rangeProbability[rightBoundIndex];
            } else {
                int left = (Integer) leftKey;
                int right = (Integer) rightKey;
                int range = right - left;

                if (isRightBoundHigh) {
                    range--;
                    if (range < 1) {
                        logger.error("range < 1, range: {}", range);
                    }
                }

                eqRatio = 1.0 / range;
            }
        }

        List<BucketOfSingleColumn> result = new ArrayList<>();

        if (rangeType == RangeType.EQ) {
            result.add(new BucketOfSingleColumn(rightBoundIndex, Math.min(eqRatio, 1.0)));
        } else if (rangeType == RangeType.LESS_EQ) {
            result.add(new BucketOfSingleColumn(rightBoundIndex, 1 - rightRatio));

            result.addAll(IntStream.rangeClosed(0, rightBoundIndex - 1)
                    .parallel()
                    .filter(i -> rangeProbability[i] != 0)
                    .mapToObj(i -> new BucketOfSingleColumn(i, 1))
                    .collect(Collectors.toList()));
        } else if (rangeType == RangeType.GREATER_EQ) {
            result.add(new BucketOfSingleColumn(rightBoundIndex, Math.min(rightRatio + eqRatio, 1.0)));

            result.addAll(IntStream.range(rightBoundIndex + 1, rangeProbability.length - 1)
                    .parallel()
                    .filter(i -> rangeProbability[i] != 0)
                    .mapToObj(i -> new BucketOfSingleColumn(i, 1))
                    .collect(Collectors.toList()));
        }

        return result;
    }


    /**
     * 返回单列查询概率
     * @param inputValue
     * @param rangeType
     * @return
     */
    public HistogramRequest getHistogramRequest(String inputValue, RangeType rangeType) {
        T value = columnTypeAdaptor.transform(inputValue);
        // value比现在的直方图里的值都要小，返回概率0
        if (value.compareTo(domain.firstKey()) < 0) {
            return new HistogramRequest(-1, 0, rangeType);
        }
        // value比现在的直方图里的值都要大，因此返回最大的概率
        if (value.compareTo(domain.lastKey()) > 0) {
            return new HistogramRequest(rangeProbability.length - 1, 0, rangeType);
        }
        // ceilingEntry返回大于或等于给定键的最小键。如果没有这样的键，则返回 null
        var rightBound = domain.ceilingEntry(value);
        // 计算value到右边界的比例
        double rightRatio = 0;
        // lowerKey返回的是严格小于给定键的最大键
        T leftBoundKey = domain.lowerKey(value);
        if (highFrequencyItems.contains(value)) {
            rightRatio = 0.0;
        } else if (leftBoundKey != null && rightBound != null) {
            T rightKey = rightBound.getKey();
            boolean rightIsHigh = highFrequencyItems.contains(rightKey);

            // 只有在「右边界是高频项」且「该类型支持 open-bound 修正」时才修正
            if (rightIsHigh && !(columnTypeAdaptor instanceof StringAdaptor)) {
                // int / date
                rightRatio = columnTypeAdaptor.ratioInRightOpenInterval(
                        leftBoundKey, rightKey, value);
            } else {
                // 字符串，或普通 rightBound
                rightRatio = columnTypeAdaptor.computeRatioFromValueToRightBound(
                        leftBoundKey, rightKey, value);
            }
        }
        double eqRatio = 1.0;
        int rightBoundIndex = rightBound.getValue();
        // 如果value不是高频项，则计算在这个range的比例
        if (!highFrequencyItems.contains(value)) {
            boolean isRightBoundHighFrequencyItem = false;
            if (highFrequencyItems.size() + 1 == rangeProbability.length) {
                throw new IllegalStateException("该直方图为纯高频项直方图，但是高频项中不存在该值, 无法判定其概率");
            }
            // 如果右边界为高频项，移动到左边的range, 否则不需要移动
            if (highFrequencyItems.contains(rightBound.getKey())) {
                rightBoundIndex--;
                isRightBoundHighFrequencyItem = true;
            }
            if(columnTypeAdaptor instanceof StringAdaptor){
                if(rangeProbability[rightBoundIndex] > 0.0){
                    eqRatio = avgProbabilityForNormalFrequencyItem / rangeProbability[rightBoundIndex];
                }else {
                    eqRatio = 0.0;
                }
            }else {
                int range = (Integer) rightBound.getKey() - (Integer) leftBoundKey;
                if(isRightBoundHighFrequencyItem){
                    range = range - 1;
                }
                if (range <= 0) {
                    logger.error(
                            "[EQ-RATIO-INVALID] range <= 0\n"
                                    + "  inputValue={}\n"
                                    + "  rangeType={}\n"
                                    + "  leftBoundKey={}\n"
                                    + "  rightBoundKey={}\n"
                                    + "  isRightBoundHighFrequencyItem={}\n"
                                    + "  rightBoundIndex(before)={}\n"
                                    + "  highFrequencyItems.contains(value)={}\n"
                                    + "  highFrequencyItems.contains(rightBoundKey)={}\n"
                                    + "  rangeProbability[rightBoundIndex]={}",
                            inputValue,
                            rangeType,
                            leftBoundKey,
                            rightBound.getKey(),
                            isRightBoundHighFrequencyItem,
                            rightBoundIndex,
                            highFrequencyItems.contains(value),
                            highFrequencyItems.contains(rightBound.getKey()),
                            rangeProbability[rightBoundIndex]
                    );
                }
                eqRatio = 1.0 / range;
            }
        }
        // 记录来自查询的概率需求到生成器
        double currentRangeRatio = switch (rangeType) {
            case LESS_EQ -> 1 - rightRatio;
            case GREATER_EQ -> Math.min(rightRatio + eqRatio, 1); // 防止概率和超过1
            case EQ -> Math.min(eqRatio, 1);
        };

        if (Double.isNaN(currentRangeRatio)
                || Double.isInfinite(currentRangeRatio)
                || currentRangeRatio < 0
                || currentRangeRatio > 1) {
            logger.error(
                    "[CURRENT-RANGE-RATIO-INVALID]\n"
                            + "  inputValue={}\n"
                            + "  rangeType={}\n"
                            + "  currentRangeRatio={}\n"
                            + "  eqRatio={}\n"
                            + "  rightRatio={}\n"
                            + "  rightBoundIndex={}\n"
                            + "  leftBoundKey={}\n"
                            + "  rightBoundKey={}",
                    inputValue,
                    rangeType,
                    currentRangeRatio,
                    eqRatio,
                    rightRatio,
                    rightBoundIndex,
                    leftBoundKey,
                    rightBound.getKey()
            );
        }

        HistogramRequest req =
                new HistogramRequest(rightBoundIndex, currentRangeRatio, rangeType);

        logger.debug("HistogramRequest: value={}, req={}", inputValue, req);

        return req;
//        return new HistogramRequest(rightBoundIndex, currentRangeRatio, rangeType);
    }

    public int getDomainSize() {
        return domain.size() + (rangeProbability[rangeProbability.length - 1] > 0 ? 1 : 0);
    }

    public int getHistogramSize() {
        return rangeProbability.length;
    }

    /**
     * 获取Metis统计的MCV值（包含了 从等频直方图中提取的）
     * @return
     */
    public int getMcvSize() {
        return highFrequencyItems.size();
    }

    public int getNdvSize() {
        return ndv;
    }

    public ColumnTypeAdaptor<T> getColumnTypeAdaptor() {
        return columnTypeAdaptor;
    }

    public Iterator<Map.Entry<HistogramRequest, Double>> iterator() {
        return new HistogramRequestIterator();
    }

    private class HistogramRequestIterator implements Iterator<Map.Entry<HistogramRequest, Double>> {
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            return currentIndex < rangeProbability.length;
        }

        @Override
        public Map.Entry<HistogramRequest, Double> next() {
            if (hasNext()) {
                HistogramRequest histogramRequest = new HistogramRequest(currentIndex, 1, RangeType.EQ);
                return new AbstractMap.SimpleEntry<>(histogramRequest, rangeProbability[currentIndex++]);
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    /**
     * 存储需要持久化的信息
     * @return
     */
    public Map<String, Object> exportStorageInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("domain", new TreeMap<>(domain));
        info.put("rangeProbability", Arrays.copyOf(rangeProbability, rangeProbability.length));
        return info;
    }

    public void debugPrintHistogramBySemantic() {
        logger.debug("========== Single Column Histogram (Semantic View) ==========");
        logger.debug("High Frequency Items (MCV): {}", highFrequencyItems);

        // 反向索引：bucketIndex -> key
        Map<Integer, T> index2Key = new HashMap<>();
        for (Map.Entry<T, Integer> e : domain.entrySet()) {
            index2Key.put(e.getValue(), e.getKey());
        }

        for (int i = 0; i < rangeProbability.length; i++) {
            double prob = rangeProbability[i];
            if (prob == 0.0) {
                continue;
            }

            String semantic;

            // 1. NULL bucket
            if (i == rangeProbability.length - 1) {
                semantic = "NULL";
            }
            // 2. MCV 点
            else if (index2Key.containsKey(i)
                    && highFrequencyItems.contains(index2Key.get(i))) {

                T v = index2Key.get(i);
                semantic = String.format("MCV = %s", v);
            }
            // 3. 区间 bucket
            else {
                T right = null;
                for (Map.Entry<T, Integer> e : domain.entrySet()) {
                    if (e.getValue() == i || e.getValue() - 1 == i) {
                        right = e.getKey();
                        break;
                    }
                }

                if (right == null) {
                    semantic = "UNKNOWN";
                } else {
                    T left = domain.lowerKey(right);

                    if (highFrequencyItems.contains(right)) {
                        // (l, r)
                        semantic = String.format("(%s, %s)", left, right);
                    } else {
                        // (l, r]
                        semantic = String.format("(%s, %s]", left, right);
                    }
                }
            }

            logger.debug(
                    "bucketIndex={}, prob={}, semantic={}",
                    i, prob, semantic
            );
        }
    }

}
