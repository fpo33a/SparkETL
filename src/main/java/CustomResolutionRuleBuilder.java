import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import scala.Function1;

public class CustomResolutionRuleBuilder implements Function1<SparkSession, Rule<LogicalPlan>> {


    @Override
    public Rule<LogicalPlan> apply(SparkSession v1) {
        return new CustomResolutionRule(v1);
    }
}
