package ecnu.db.correlation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

import com.gurobi.gurobi.*;

public class ColumnCorrelationCPSolver {
    private static final Logger logger = LoggerFactory.getLogger(ColumnCorrelationCPSolver.class);

    private static final double SCALE_FACTOR = 1e8;
    private static final int TOTAL_COST_LIMIT_ = 20000;
    private static final long TOTAL_COST_LIMIT = transferColumnSizeToCost(TOTAL_COST_LIMIT_);

    private ColumnCorrelationCPSolver() {
    }

    private static long transferColumnSizeToCost(int columnSize) {
        return Math.round(Math.log(columnSize) * SCALE_FACTOR);
    }

    /**
     * 选择要构建的列簇
     * 约束1：选择的列簇中至少有一个未见过的列
     * 约束2：选择的列簇对应的联合域空间大小不超过固定上限（TOTAL_COST_LIMIT）
     * 目标：选择权重和最大的可解子图
     *
     * @param histogramSizes                 直方图大小
     * @param columnPair2CumulativeFrequency
     * @return 选择的列索引
     */
    public static List<Integer> chooseCorrelatedColumnIndex(
            int[] histogramSizes,
            Map<Set<Integer>, Integer> columnPair2CumulativeFrequency,
            Set<Set<Integer>> alreadyConstructedColumnIndexSets
    ) {
        GRBEnv env = null;
        GRBModel model = null;

        try {
            env = new GRBEnv(true);
            env.set(GRB.IntParam.Threads, 1);
            if(logger.isDebugEnabled()){
                env.set(GRB.IntParam.OutputFlag, 1);
            }else {
                env.set(GRB.IntParam.OutputFlag, 0);
            }
            env.start();

            model = new GRBModel(env);

            int n = histogramSizes.length;

            GRBVar[] columnVars = new GRBVar[n];
            long[] costs = new long[n];

            for (int i = 0; i < n; i++) {
                columnVars[i] = model.addVar(0, 1, 0, GRB.BINARY, "col_" + i);
                costs[i] = transferColumnSizeToCost(histogramSizes[i]);
            }

            // 成本约束
            GRBLinExpr costExpr = new GRBLinExpr();
            for (int i = 0; i < n; i++) {
                costExpr.addTerm(costs[i], columnVars[i]);
            }
            model.addConstr(costExpr, GRB.LESS_EQUAL, TOTAL_COST_LIMIT, "cost");

            // 禁止新选择列簇 S 成为任意已构建列簇 C 的子集：S ⊄ C
            for (Set<Integer> cset : alreadyConstructedColumnIndexSets) {
                GRBLinExpr outsideExpr = new GRBLinExpr();
                for (int i = 0; i < n; i++) {
                    if (!cset.contains(i)) {
                        outsideExpr.addTerm(1.0, columnVars[i]);
                    }
                }
                model.addConstr(outsideExpr, GRB.GREATER_EQUAL, 1.0,
                        "not_subset_of_" + cset.hashCode());
            }

            // 边变量
            int m = columnPair2CumulativeFrequency.size();
            GRBVar[] edgeVars = new GRBVar[m];
            long[] weights = new long[m];

            int idx = 0;

            for (var entry : columnPair2CumulativeFrequency.entrySet()) {
                // ！！！关键修复：必须保证两个元素的顺序可控
                List<Integer> pair = new ArrayList<>(entry.getKey());
                if (pair.size() != 2) {
                    throw new IllegalStateException("Column pair must contain exactly 2 indices.");
                }
                int u = pair.get(0);
                int v = pair.get(1);

                GRBVar uVar = columnVars[u];
                GRBVar vVar = columnVars[v];

                GRBVar eVar = model.addVar(0, 1, 0, GRB.BINARY,
                        "edge_" + u + "_" + v);
                edgeVars[idx] = eVar;
                weights[idx] = entry.getValue();

                // 线性化 edge = u AND v
                model.addConstr(eVar, GRB.LESS_EQUAL, uVar, "e_le_u_" + idx);
                model.addConstr(eVar, GRB.LESS_EQUAL, vVar, "e_le_v_" + idx);

                GRBLinExpr expr2 = new GRBLinExpr();
                expr2.addTerm(1.0, uVar);
                expr2.addTerm(1.0, vVar);
                expr2.addConstant(-1.0);
                model.addConstr(eVar, GRB.GREATER_EQUAL, expr2, "e_ge_uv_" + idx);

                idx++;
            }

            // objective
            GRBLinExpr obj = new GRBLinExpr();
            for (int i = 0; i < m; i++) obj.addTerm(weights[i], edgeVars[i]);
            for (int i = 0; i < n; i++) obj.addTerm(1.0, columnVars[i]);
            obj.addConstant(-1.0);

            model.setObjective(obj, GRB.MAXIMIZE);
            model.optimize();

            int status = model.get(GRB.IntAttr.Status);
            if (status != GRB.Status.OPTIMAL && status != GRB.Status.SUBOPTIMAL) {
                logger.error("Gurobi failed with status: " + status);
                return Collections.emptyList();
            }

            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (columnVars[i].get(GRB.DoubleAttr.X) > 0.5) {
                    result.add(i);
                }
            }

            return result;

        } catch (Exception e) {
            logger.error("Gurobi error in chooseCorrelatedColumnIndex", e);
            throw new RuntimeException(e);

        } finally {
            // 安全释放：强烈建议
            try {
                if (model != null) model.dispose();
                if (env != null) env.dispose();
            } catch (GRBException ignored) {
            }
        }
    }
}
