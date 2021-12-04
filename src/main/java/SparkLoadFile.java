/*

Basic example of spark ETL job reading data in csv and loading it into a relational DB (H2) table after 1 VALUE simple transformation
( transformation = 'data' field converted to uppercase )

This example requires to have some spark classes in class path
  in intellij add project module dependencies to directory C:\frank\spark-2.3.0-bin-hadoop2.7\jars & to h2 jar file h2.2.0.202.jar

1/ Data to load
C:\windows\system32>more c:\temp\data.csv
id;parentid;data;date;url
1;1;this is line 1;12/21/2021;www.line1.com
2;1;this is line 2;12/22/2021;www.line2.com
3;2;this is line 3;12/23/2021;www.line3.com
4;2;this is line 4;12/24/2021;www.line4.com

2/ execution
C:\frank\SparkLoadFile\target>java -cp "SparkLoadFile-1.0-SNAPSHOT-jar-with-dependencies.jar;C:\frank\spark-2.3.0-bin-hadoop2.7\jars\*" SparkLoadFile

[...]
2021-12-03 16:02:47 INFO  DAGScheduler:54 - Job 1 finished: load at SparkLoadFile.java:28, took 0,213672 s
Schema:
root
 |-- id: integer (nullable = true)
 |-- parentid: integer (nullable = true)
 |-- data: string (nullable = true)
 |-- date: string (nullable = true)
 |-- url: string (nullable = true)
[...]
2021-12-03 16:02:49 INFO  DAGScheduler:54 - Job 2 finished: show at SparkLoadFile.java:34, took 0,991968 s
+---+--------+--------------+----------+-------------+
| id|parentid|          data|      date|          url|
+---+--------+--------------+----------+-------------+
|  1|       1|this is line 1|12/21/2021|www.line1.com|
|  2|       1|this is line 2|12/22/2021|www.line2.com|
|  3|       2|this is line 3|12/23/2021|www.line3.com|
|  4|       2|this is line 4|12/24/2021|www.line4.com|
+---+--------+--------------+----------+-------------+



3/ H2 setup:

@java -cp "h2-2.0.202.jar;%H2DRIVERS%;%CLASSPATH%" org.h2.tools.Server %*
@if errorlevel 1 pause

drop table data;
create table data (
id INT ,
parent_id INT,
data VARCHAR(255),
dt VARCHAR(20),
url VARCHAR(255) );

4/ check result in H2:

select * from data;

1	1	THIS IS LINE 1	12/21/2021	www.line1.com
2	1	THIS IS LINE 2	12/22/2021	www.line2.com
3	2	THIS IS LINE 3	12/23/2021	www.line3.com
4	2	THIS IS LINE 4	12/24/2021	www.line4.com
*/


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class SparkLoadFile {

    //--------------------------------------------------------------

    public static void main(String[] args) {
        SparkLoadFile sparkLoadFile = new SparkLoadFile();
        sparkLoadFile.start();

    }

    //--------------------------------------------------------------

    void start() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkLoadFile")
                .master("local")
                .getOrCreate();

        Dataset<Row> extractedData = this.extract(spark, "C:\\temp\\data.csv");
        Dataset<Row> transformedData = this.transform(extractedData);
        this.load(transformedData);
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
                .load(filename);

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        df.show();
        return df;
    }

    //--------------------------------------------------------------

    Dataset<Row> transform(Dataset<Row> dataset) {

        Tuple2<String, String>[] dt = dataset.dtypes();

        // buid structure based on input dataset ( only works if transformation doesn't modify any column  type)
        StructType structType = new StructType();
        for (int i = 0; i < dt.length; i++) {
            Tuple2<String, String> tuple = dt[i];
            switch (tuple._2()) {
                case "IntegerType":
                    structType = structType.add(tuple._1(), DataTypes.IntegerType, true);
                    break;

                case "StringType":
                    structType = structType.add(tuple._1(), DataTypes.StringType, true);
                    break;

                case "TimestampType":
                    structType = structType.add(tuple._1(), DataTypes.TimestampType, true);
                    break;

                default:
                    break;
            }
        }
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        return dataset.map(
                (MapFunction<Row, Row>) input -> RowFactory.create(input.getInt(0),
                        input.getInt(1),
                        input.getString(2).toUpperCase(),
                        input.getString(3),
                        input.getString(4)
                ), encoder
        );
    }
    //--------------------------------------------------------------

    void load(Dataset<Row> dataset) {

        String url = "jdbc:h2:tcp://localhost/~/test";

        try {
            Class.forName("org.h2.Driver");
            Properties props = new Properties();
            props.put("user", "sa");
            props.put("password", "");
            Connection conn = DriverManager.getConnection(url, props);

            String SQL_INSERT = "INSERT INTO DATA (ID, PARENT_ID, DATA, DT, URL) VALUES (?,?,?,?,?)";

            PreparedStatement preparedStatement = conn.prepareStatement(SQL_INSERT);

            List<Row> arrayList = new ArrayList<>();
            arrayList = dataset.collectAsList();

            for (int i = 0; i < arrayList.size(); i++) {

                Row record = arrayList.get(i);
                StructType schema = record.schema();

                // base on record info build jdbc statement fields dynamically
                for (int j = 0; j < schema.fields().length; j++) {
                    String typeName = schema.fields()[j].dataType().typeName();
                    switch (typeName) {
                        case "integer":
                            preparedStatement.setInt(j+1, record.getInt(j));
                            break;

                        case "string":
                            preparedStatement.setString(j+1, record.getString(j));
                            break;

                        case "timestamp":
                            preparedStatement.setTimestamp (j+1, record.getTimestamp(j));
                            break;

                        default:
                            break;
                    }
                }
                int row = preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    //--------------------------------------------------------------

}
