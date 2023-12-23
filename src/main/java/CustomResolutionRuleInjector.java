import org.apache.spark.sql.SparkSessionExtensions;
import scala.Function1;
import scala.runtime.BoxedUnit;


public class CustomResolutionRuleInjector implements Function1<SparkSessionExtensions, BoxedUnit> {
    @Override
    public BoxedUnit apply(SparkSessionExtensions sparkSessionExtensions) {
        System.out.println("   -----> CustomResolutionRuleInjector.apply");
        sparkSessionExtensions.injectResolutionRule(new CustomResolutionRuleBuilder());
        return BoxedUnit.UNIT;
    }
}
