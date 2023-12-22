import org.apache.spark.sql.SparkSessionExtensions;
import scala.Function1;
import scala.runtime.BoxedUnit;

public class CustomParserInjector implements Function1<SparkSessionExtensions, BoxedUnit> {
    @Override
    public BoxedUnit apply(SparkSessionExtensions sparkSessionExtensions) {
        System.out.println("   -----> InjectParser.apply");
        sparkSessionExtensions.injectParser(new CustomParserBuilder());
        return BoxedUnit.UNIT;
    }
}
