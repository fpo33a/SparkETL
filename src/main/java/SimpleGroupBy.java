// compare groupby aggregate & window functions

/* executions time on home laptop with spark 2.3.0

1/ with simple repartition (10) ( no column )
- groupby : 51 sec
- window  : 37 sec

2/ with repartition on agg column (10, col("parentid")
- groupby : 38 sec
- window  : 8 sec

 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import java.util.Date;

import static org.apache.spark.sql.functions.col;


public class SimpleGroupBy {

    Date d0;
    Date d1;
    Date d2;
    Date d3;

    //--------------------------------------------------------------

    public static void main(String[] args) {
        SimpleGroupBy SimpleGroupBy = new SimpleGroupBy();
        SimpleGroupBy.start();
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
                .appName("SimpleGroupBy")
                .master("local")
                .getOrCreate();

        System.out.println("----------------------------------------------------------");
        System.out.println("load data file ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> extractedData = this.extract(spark, "C:\\temp\\data.csv");

        System.out.println("----------------------------------------------------------");
        System.out.println("simpleGroupByKeyData ");
        System.out.println("----------------------------------------------------------");
        d0 = new Date();
        this.simpleGroupByKeyData(extractedData);
        d1 = new Date();

        System.out.println("----------------------------------------------------------");
        System.out.println("windowGroupByKeyData ");
        System.out.println("----------------------------------------------------------");
        d2 = new Date();
        this.windowGroupByKeyData(extractedData);
        d3 = new Date();

        System.out.println("----------------------------------------------------------");
        long t1 = d1.getTime() - d0.getTime();
        long t2 = d3.getTime() - d2.getTime();
        System.out.println("simpleGroupByKeyData = " + t1 + ", windowGroupByKeyData " + t2);
        System.out.println("----------------------------------------------------------");

        wait(600);

    }

    //--------------------------------------------------------------

    Dataset<Row> extract(SparkSession spark, String filename) {

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("multiline", false)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load(filename)
                //.repartition(10)
                .repartition(10, col("parentid"))
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

    Dataset<Row> simpleGroupByKeyData(Dataset<Row> dataset) {

        System.out.println("----------------------------------------------------------");
        System.out.println("simpleGroupByKeyData starting " + new java.util.Date());
        System.out.println("----------------------------------------------------------");


        Dataset<Row> gbDs = dataset.groupBy(col("parentid")).count().withColumnRenamed("parentid", "pid");
        Dataset<Row> result = dataset.join(gbDs, dataset.col("parentId").equalTo(gbDs.col("pid")), "inner").drop("pid");
        result.write().option("header", true).mode("overwrite").csv("c:/temp/simple.csv");


        System.out.println("----------------------------------------------------------");
        System.out.println("simpleGroupByKeyData ending " + new java.util.Date());
        System.out.println("----------------------------------------------------------");
        return result;

    }

    //--------------------------------------------------------------

    Dataset<Row> windowGroupByKeyData(Dataset<Row> dataset) {

        System.out.println("----------------------------------------------------------");
        System.out.println("windowGroupByKeyData starting " + new java.util.Date());
        System.out.println("----------------------------------------------------------");


        Dataset<Row> result = dataset.withColumn("count", functions.count("*").over(Window.partitionBy("parentid")));
        result.write().option("header", true).mode("overwrite").csv("c:/temp/window.csv");


        System.out.println("----------------------------------------------------------");
        System.out.println("windowGroupByKeyData ending " + new java.util.Date());
        System.out.println("----------------------------------------------------------");
        return result;

    }

    //--------------------------------------------------------------

}

