package ecnu.db;

import com.gurobi.gurobi.*;

public class TestGurobi {

    public static void main(String[] args) {
        try {
            GRBEnv env = new GRBEnv(true);
            env.set("logFile", "test.log");
            env.start();

            GRBModel model = new GRBModel(env);

            GRBVar x = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, "x");
            GRBVar y = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, "y");

            model.addConstr(x, GRB.LESS_EQUAL, y, "c1");

            model.optimize();

            model.dispose();
            env.dispose();

        } catch (GRBException e) {
            e.printStackTrace();
        }
    }
}
