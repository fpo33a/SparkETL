import java.io.FileWriter;
import java.io.IOException;

public class GenerateData {

    public static void main (String args []) {
        try {
            FileWriter fileWriter = new FileWriter("c:/temp/data.csv");
            String line = "";
            fileWriter.write("id;parentid;data;date;url\n");
            for (int i = 0; i < 10000; i++) {
                line = i+";"+(i%100)+";this is line "+i+";12/"+(i%30)+"/2021;www.line"+i+".com\n";
                fileWriter.write(line);
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
