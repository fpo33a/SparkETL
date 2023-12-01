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
import java.util.Date;
import static org.apache.spark.sql.functions.col;

public class AccountGroupBy {

    Date d0;
    Date d1;

    //--------------------------------------------------------------

    public static void main(String[] args) {
        AccountGroupBy accountGroupBy = new AccountGroupBy();
        accountGroupBy.start();
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
        SparkSession spark = SparkSession.builder()
                .appName("accountGroupBy")
                .master("local")
                .getOrCreate();

        System.out.println("----------------------------------------------------------");
        System.out.println("load data file ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> extractedData = this.extract(spark, "C:\\temp\\accounts.csv");

        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy ");
        System.out.println("----------------------------------------------------------");
        d0 = new Date();
        Dataset<Row> result = this.accountGroupBy(extractedData);
        d1 = new Date();
        result.show(20, false);

        System.out.println("----------------------------------------------------------");
        long t1 = d1.getTime() - d0.getTime();
        System.out.println("accountGroupBy = " + t1);
        System.out.println("----------------------------------------------------------");

        wait(600);

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
                //.repartition(10, col("fromAcc"))
                .cache();

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        //this.wait(60);
        df.show();
        //this.wait(60);
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> accountGroupBy(Dataset<Row> dataset) {

        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy starting " + new java.util.Date());
        System.out.println("----------------------------------------------------------");


        //Dataset<Row> result = dataset.groupBy(col("parentid")).count().withColumnRenamed("parentid", "pid");
        Dataset<Row> result = dataset
                .rollup(col("fromAcc"), col("year"), col("month"), col("day"), col("hour"))
                .sum("amount")
                .select("fromAcc", "year", "month", "day", "hour", "sum(amount)")
                //.where(col("hour").isNotNull())
                .sort("year", "month", "day", "hour", "fromAcc");
        result.write().option("header", true).mode("overwrite").csv("c:/temp/accresult.csv");


        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy ending " + new java.util.Date());
        System.out.println("----------------------------------------------------------");
        return result;

    }

    //--------------------------------------------------------------

}

