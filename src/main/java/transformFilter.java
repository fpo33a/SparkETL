import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.*;

// example of Filter value transformation

public class transformFilter implements scala.PartialFunction<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> {
    @Override
    public boolean isDefinedAt(LogicalPlan x) {
        if (x instanceof Filter)
            return true;
        return false;
    }

    @Override
    public LogicalPlan apply(LogicalPlan v1) {
        if (v1 instanceof Filter) {
            Filter f = (Filter) v1;
            Expression c = f.condition();
            if (c instanceof LessThan) {
                LessThan lt = (LessThan) c;
                Expression l = lt.left();
                AttributeReference nar = null;
                if (l instanceof AttributeReference) {
                    AttributeReference ar = (AttributeReference) l;
                    // if next line uncommented rename the column name to filter on ( from 'amount' to 'toto' )
                    // nar = ar.withName("toto");
                    nar = ar;
                }
                Expression r = lt.right();
                if (r instanceof Literal) {
                    Literal lit = (Literal) r;
                }
                String s = lt.symbol();
                System.out.println ("   ---> transformFilter: left : "+l.toString()+", right : "+r.toString()+", symbol : "+s);
                // change original '100' filter value by '324'
                LessThan nlt = new LessThan(nar, new Literal(324, DataTypes.IntegerType));
                return new Filter(nlt, f.child());
            }
        }
        return v1;
    }
}
