/* dataset

fromAcc,toAcc,Amount,year,month,day,hour,min,communication
acc000001,acc000002,12,2023,01,01,12,00,test 1-2
acc000001,acc000003,13,2023,01,01,12,00,test 1-3
acc000001,acc000004,14,2023,01,01,12,00,test 1-4
acc000003,acc000002,32,2023,01,01,12,00,test 3-2
acc000003,acc000004,34,2023,01,01,12,00,test 3-4
acc000001,acc000002,120,2023,01,01,13,00,test 1-2
acc000001,acc000003,130,2023,01,01,13,00,test 1-3
acc000004,acc000002,420,2023,01,01,13,00,test 4-2
acc000003,acc000002,320,2023,01,01,13,00,test 3-2
acc000002,acc000001,210,2023,01,01,13,00,test 2-1

 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;

import java.util.Date;
import static org.apache.spark.sql.functions.col;

public class CustomParserTest {

    //--------------------------------------------------------------

    public static void main(String[] args) {
        CustomParserTest test = new CustomParserTest();
        test.start();
    }

    //--------------------------------------------------------------

    private void wait(int sec) {
        try {
            Thread.sleep(1000 * sec);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //--------------------------------------------------------------

    void start() {
        SparkConf conf = new SparkConf()
                .setAppName("test")
                .setMaster("local[4]");

        SparkSession spark = SparkSession.builder().config(conf)
                .withExtensions( new CustomParserInjector() )                   // add specific parser extension
                .getOrCreate();

        // to test parser interceptor we need some "spark.sql" code
        Dataset<Row> test = spark.sql("select * from range(10)");
        test.explain(true);
        test.show();

        wait(600);

    }

    //--------------------------------------------------------------

}

