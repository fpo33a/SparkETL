import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

public class CustomParser implements ParserInterface {

    private ParserInterface parserInterface;

    public CustomParser(ParserInterface parserInterface) {
        System.out.println("   -----> StrictParser constructor");
        this.parserInterface = parserInterface;
    }
    @Override
    public LogicalPlan parsePlan(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parsePlan "+sqlText);
        return parserInterface.parsePlan(sqlText);
      //  return null;
    }

    @Override
    public Expression parseExpression(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseExpression "+sqlText);
        return parserInterface.parseExpression(sqlText);
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseTableIdentifier "+sqlText);
        return parserInterface.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseFunctionIdentifier "+sqlText);
        return parserInterface.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseMultipartIdentifier "+sqlText);
        return parserInterface.parseMultipartIdentifier(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseTableSchema "+sqlText);
        return parserInterface.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText) throws ParseException {
        System.out.println("   -----> StrictParser parseDataType "+sqlText);
        return parserInterface.parseDataType(sqlText);
    }
}
