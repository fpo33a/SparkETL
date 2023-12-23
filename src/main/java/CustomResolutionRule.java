import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.Seq;

public class CustomResolutionRule extends Rule<LogicalPlan> {
    public CustomResolutionRule(SparkSession v1) {
        System.out.println("   ---> CustomResolutionRule constructor");

    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        //System.out.println(  "   -----> " + plan.toString());
        //LogicalPlan newPlan = plan.transformDown(new transform());
        /*
        Iterator<LogicalPlan> childItr = newPlan.children().iterator();
        while (childItr.hasNext()) {
            LogicalPlan logicalPlan = childItr.next();
            System.out.println(  "   ***-----> " + logicalPlan.toString());
         }

         */
        if (plan instanceof LogicalRelation) {
            LogicalRelation lr = (LogicalRelation) plan;
            if (lr.relation() instanceof  HadoopFsRelation ) {
                HadoopFsRelation relation = (HadoopFsRelation) lr.relation();
                String[] files = relation.location().inputFiles();
                StructType columns = relation.dataSchema();
                System.out.println("   ***** hadoopFsRelation path  -----> " + relation.location().rootPaths().toString());
                for (int i = 0; i < files.length; i++)
                    System.out.println("   ***** hadoopFsRelation file ["+i+"]  -----> " + relation.location().inputFiles()[i]);
                StructField[] fields = columns.fields();
                for (int i = 0; i < fields.length; i++)
                    System.out.println("   ***** hadoopFsRelation schema field ["+i+"] -----> " + fields[i].name()+" : "+fields[i].dataType());
            }
        }
        else if (plan instanceof Project) {
            Project p = (Project) plan;
            Seq<NamedExpression> list = p.projectList();
            System.out.println("   ***** project -----> " + p.toString());
            Iterator<NamedExpression> iterator = list.iterator();
            while (iterator.hasNext()) {
                NamedExpression seq = iterator.next();
                System.out.println("   **** project element ----> : "+seq.name());
                if (seq.name().compareToIgnoreCase("amount") == 0) {
                    throw new AuthorizationException("You are not authorized to read this column");
                }
            }
        }
        else if (plan instanceof Filter) {
            Filter f = (Filter) plan;
            System.out.println("   ***** filter -----> " + f.toString());
        }
        return plan;
    }
}
