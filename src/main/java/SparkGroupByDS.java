/*

Basic example of spark job doing group by operation

This example requires to have some spark classes in class path
  in intellij add project module dependencies to directory C:\frank\spark-2.3.0-bin-hadoop2.7\jars

1/ Data to load
C:\windows\system32>more c:\temp\data.csv
id;parentid;data;date;url
1;1;this is line 1;12/21/2021;www.line1.com
2;1;this is line 2;12/22/2021;www.line2.com
3;2;this is line 3;12/23/2021;www.line3.com
4;2;this is line 4;12/24/2021;www.line4.com

2/ execution
C:\frank\SparkLoadFile\target>java -cp "SparkLoadFile-1.0-SNAPSHOT-jar-with-dependencies.jar;C:\frank\spark-2.3.0-bin-hadoop2.7\jars\*" SparkGroupBy

[...]
*/

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;


public class SparkGroupByDS {

    //--------------------------------------------------------------

    public static void main(String[] args) {
        SparkGroupByDS sparkGroupByDS = new SparkGroupByDS();
        sparkGroupByDS.start();
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
                .appName("SparkGroupBy")
                .master("local")
                .getOrCreate();

        System.out.println("----------------------------------------------------------");
        System.out.println("load data file ");
        System.out.println("----------------------------------------------------------");
        Dataset<MyRecord> extractedData = this.extract(spark, "C:\\temp\\data.csv");

        System.out.println("----------------------------------------------------------");
        System.out.println("groupByKeyAndReduceData ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> res1 = this.groupByKeyAndReduceData(extractedData);
        res1.show(30,false);

        System.out.println("----------------------------------------------------------");
        System.out.println("groupByData ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> res2 = this.groupByData(extractedData);
        res2.show(30, false);
        System.out.println("nb part = "+res2.rdd().getNumPartitions());

        System.out.println("----------------------------------------------------------");
        System.out.println("groupBySqlData ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> res3 = this.groupBySqlData(spark,extractedData);
        res3.show(30, false);

        wait(600);


    }

    //--------------------------------------------------------------

    Dataset<MyRecord> extract(SparkSession spark, String filename) {

        Dataset<MyRecord> df = spark.read()
                .option("header", true)
                .option("multiline", false)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .csv(filename)
                .as(Encoders.bean(MyRecord.class))
                .repartition(10);

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        df.show();
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> groupByKeyAndReduceData(Dataset<MyRecord> dataset) {

        // the KeyValueGroupedDataset will contain a key ( third column of dataset ) + list of all rows for that key
        KeyValueGroupedDataset<String, MyRecord> kvDataset = dataset.groupByKey((MapFunction<MyRecord, String>) (row) -> {
            return row.getDate();        // colum 3 = date ( group by date )
        }, Encoders.STRING());

        // Note: for learning purpose we do two different dataset ( resultGroups & result ). Both operations could have been combined
        //       but for learning ( using debugger ) we split ( to see intermediate structure )
        //       The fact it is split doesn't change anything to plan nor perfs ( tested & compared )

        // create dataset of  <date, row of max parentid ROW per date>
        Dataset<Tuple2<String, MyRecord>> reduceGroups = kvDataset.reduceGroups((ReduceFunction<MyRecord>) (v1, v2) -> {
            // System.out.println("v1 =  " + v1.toString() + ", v2 = " + v2.toString());
            if (v1.getParentid() > v2.getParentid()) return v1;
            return v2;
        });
        //reduceGroups.show(30,false);
        //return null;

        // build a dataset<Row> with date & max parentid from previous ds
        StructType structType = new StructType();
        structType = structType.add("date", DataTypes.StringType, false);
        structType = structType.add("parentid", DataTypes.IntegerType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> result = reduceGroups.map((MapFunction<Tuple2<String, MyRecord>, Row>) (tuple) -> {
            MyRecord row = tuple._2();
            return RowFactory.create(row.getDate(), row.getParentid());
        }, encoder);

        result.explain(true);
        return result;

    }

    //--------------------------------------------------------------

    Dataset<Row> groupByData(Dataset<MyRecord> dataset) {

        RelationalGroupedDataset result = dataset.groupBy(col("date"));
        result.max("parentid").explain(true);
        return result.max("parentid"); // count();
    }

    //--------------------------------------------------------------

    Dataset<Row> groupBySqlData(SparkSession spark, Dataset<MyRecord> dataset) {

        dataset.createOrReplaceTempView("data");
        Dataset<Row> result = spark.sql("SELECT date, max(parentid) FROM data group by date");
        result.explain(true);
        return result;
    }

    //--------------------------------------------------------------

}

