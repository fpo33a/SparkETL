import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

public class CustomParserBuilder implements scala.Function2<SparkSession, ParserInterface,ParserInterface> {

    public CustomParser apply (SparkSession session, ParserInterface parserInterface) {
        System.out.println("   -----> StrictParserBuilder.apply");
        return new CustomParser(parserInterface);
    }
}
