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
        SparkConf conf = new SparkConf()
                .setAppName("accountGroupBy")
                .setMaster("local[4]")
                .set("spark.executor.memory", "10g")
                .set("spark.driver.memory", "4g");

        SparkSession spark = SparkSession.builder().config(conf)
                .getOrCreate();

        System.out.println("----------------------------------------------------------");
        System.out.println("load data file ");
        System.out.println("----------------------------------------------------------");
        // to read from csv & generate parquet version uncomment next line
        //Dataset<Row> extractedData = this.extract(spark, "C:\\temp\\billion-accounts.csv");

        // read from parquet
        Dataset<Row> extractedData = this.loadParquet(spark, "c:/temp/billion-parquet");

        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy ");
        System.out.println("----------------------------------------------------------");
        d0 = new Date();
        Dataset<Row> result = this.accountGroupBy(extractedData,"c:/temp/deltaaccresult");
        d1 = new Date();
        result.show(100, false);


        System.out.println("----------------------------------------------------------");
        long t1 = d1.getTime() - d0.getTime();
        System.out.println("accountGroupBy = " + t1+ ", generated records "+ result.count());
        System.out.println("----------------------------------------------------------");

        //this.countResult(spark,"c:/temp/accresult");

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
                .repartition(16, col("fromAcc"))
                .cache();

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        //this.wait(60);
        //df.show();
        //this.wait(60);
        df.write().mode("overwrite").format("parquet").save("c:/temp/billion-parquet");
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> loadParquet(SparkSession spark, String filename) {

        Dataset<Row> df = spark.read().format("parquet").load("c:/temp/billion-parquet");
        System.out.println("Schema:");
        df.printSchema();
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> countResult(SparkSession spark, String filename) {

        Dataset<Row> df = spark.read().format("parquet").load(filename);
        long count = df.count();
        System.out.println("count :"+count);
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> accountGroupBy(Dataset<Row> dataset,String filename) {

        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy starting " + new java.util.Date());
        System.out.println("----------------------------------------------------------");


        //Dataset<Row> result = dataset.groupBy(col("parentid")).count().withColumnRenamed("parentid", "pid");
        Dataset<Row> result = dataset
                .rollup(col("fromAcc"), col("year"), col("month"), col("day"), col("hour"))
                .sum("amount")
                .withColumnRenamed("sum(amount)","total")
                //.where(col("hour").isNotNull())
                .sort("fromAcc", "hour", "year", "month", "day");
        result.write().option("header", true).mode("overwrite").format("delta").save(filename);


        System.out.println("----------------------------------------------------------");
        System.out.println("accountGroupBy ending " + new java.util.Date());
        System.out.println("----------------------------------------------------------");
        return result;

    }

    //--------------------------------------------------------------

}

