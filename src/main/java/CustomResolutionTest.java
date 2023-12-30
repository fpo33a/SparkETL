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

import static org.apache.spark.sql.functions.col;

public class CustomResolutionTest {

    //--------------------------------------------------------------

    public static void main(String[] args) {
        CustomResolutionTest test = new CustomResolutionTest();
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
                .setAppName("CustomResolutionTest")
                .setMaster("local[4]");

        SparkSession spark = SparkSession.builder().config(conf)
                .withExtensions( new CustomResolutionRuleInjector() )                   // add specific extension
                .getOrCreate();

        // from all these options we can get projected fields or relation fields
        //Dataset<Row> test = spark.sql("select * from range(10)");
        //Dataset<Row> test = this.extract(spark, "C:\\temp\\billion-accounts.csv");
        //Dataset<Row> test = this.loadParquet(spark);
        Dataset<Row> test = this.loadParquet(spark).select("fromAcc","toAcc","Amount").where("Amount < 100");   // add where clause to check pushdown predicate

        test.explain(true);
        test.show();

        wait(600);

    }

    //--------------------------------------------------------------

    Dataset<Row> loadParquet(SparkSession spark) {

        Dataset<Row> df = spark.read().format("parquet").load("c:/temp/billion-parquet");
        System.out.println("Schema:");
        df.printSchema();
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> extract(SparkSession spark, String filename) {

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("multiline", false)
                .option("sep", ",")
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load(filename)
                //.repartition(10)
                .repartition(16, col("fromAcc"))
                .cache();

        System.out.println("Schema:");
        df.printSchema();

        return df;
    }

    //--------------------------------------------------------------

}

