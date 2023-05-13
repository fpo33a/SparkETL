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


public class SparkGroupBy {

    //--------------------------------------------------------------

    public static void main(String[] args) {
        SparkGroupBy sparkGroupBy = new SparkGroupBy();
        sparkGroupBy.start();
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
        Dataset<Row> extractedData = this.extract(spark, "C:\\temp\\data.csv");
/*
        System.out.println("----------------------------------------------------------");
        System.out.println("groupByKeyData ");
        System.out.println("----------------------------------------------------------");
        Dataset<String> byKey = this.groupByKeyData(extractedData);
        byKey.show();

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
*/
        System.out.println("----------------------------------------------------------");
        System.out.println("groupBySqlData ");
        System.out.println("----------------------------------------------------------");
        Dataset<Row> res3 = this.groupBySqlData(spark,extractedData);
        res3.show(30, false);

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
                .repartition(10);

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        df.show();
        return df;
    }

    //--------------------------------------------------------------

    Dataset<String> groupByKeyData(Dataset<Row> dataset) {

        // the KeyValueGroupedDataset will contain a key ( third column of dataset ) + list of all rows for that key
        KeyValueGroupedDataset<String, Row> kvDataset = dataset.groupByKey((MapFunction<Row, String>) (row) -> {
            return row.getString(3);        // colum 3 = date ( group by date )
        }, Encoders.STRING());

        // show count per key
        kvDataset.count().show();

        // the result dataset will only contains key - we iterates on values to display for learning purpose
        Dataset<String> flatMapped = kvDataset.mapGroups(
                (MapGroupsFunction<String, Row, String>) (key, values) -> {
                    /* for learning it shows values contains list of all rows for a given date
                    System.out.print ("*** " + key.toString() + ", " ) ;
                    while (values.hasNext()) {
                        Row v = values.next();
                        if (v != null) System.out.print (values.next()+ ", ");
                    }
                    System.out.println("");

                     */
                    return key.toString();
                }, Encoders.STRING());

        return flatMapped;
    }

    //--------------------------------------------------------------

    Dataset<Row> groupByKeyAndReduceData(Dataset<Row> dataset) {

        // the KeyValueGroupedDataset will contain a key ( third column of dataset ) + list of all rows for that key
        KeyValueGroupedDataset<String, Row> kvDataset = dataset.groupByKey((MapFunction<Row, String>) (row) -> {
            return row.getString(3);        // colum 3 = date ( group by date )
        }, Encoders.STRING());

        // Note: for learning purpose we do two different dataset ( resultGroups & result ). Both operations could have been combined
        //       but for learning ( using debugger ) we split ( to see intermediate structure )
        //       The fact it is split doesn't change anything to plan nor perfs ( tested & compared )

        // create dataset of  <date, row of max parentid ROW per date>
        Dataset<Tuple2<String, Row>> reduceGroups = kvDataset.reduceGroups((ReduceFunction<Row>) (v1, v2) -> {
            // System.out.println("v1 =  " + v1.toString() + ", v2 = " + v2.toString());
            if (v1.getInt(1) > v2.getInt(1)) return v1;
            return v2;
        });
        //reduceGroups.show(30,false);

        // build a dataset<Row> with date & max parentid from previous ds
        StructType structType = new StructType();
        structType = structType.add("date", DataTypes.StringType, false);
        structType = structType.add("parentid", DataTypes.IntegerType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> result = reduceGroups.map((MapFunction<Tuple2<String, Row>, Row>) (tuple) -> {
            Row row = tuple._2();
            return RowFactory.create(row.getString(3), row.getInt(1));
        }, encoder);

        return result;
    }

    //--------------------------------------------------------------

    Dataset<Row> groupByData(Dataset<Row> dataset) {

        RelationalGroupedDataset res = dataset.groupBy(col("date"));
        return res.max("parentid"); // count();
    }

    //--------------------------------------------------------------

    Dataset<Row> groupBySqlData(SparkSession spark, Dataset<Row> dataset) {

        dataset.createOrReplaceTempView("data");
        Dataset<Row> result = spark.sql("SELECT date, max(parentid) FROM data group by date");
        return result;
    }
    //--------------------------------------------------------------

}

