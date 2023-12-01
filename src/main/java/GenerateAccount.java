import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateAccount {

    public static void main (String args []) {
        try {
            FileWriter fileWriter = new FileWriter("c:/temp/billion-accounts.csv");
            String line = "";
            int totalHour = 0;
            StringBuilder sb = new StringBuilder();
            Random randomNum = new Random();
            fileWriter.write("fromAcc,toAcc,Amount,year,month,day,hour,min,communication\n");
            for (int i = 0; i < 1000000000; i++) {
                if (i % 1000000 == 0) System.out.println(i);
                String fromAcc = String.format("\"%09d\"", randomNum. nextInt(10000000));
                sb.append(fromAcc);
                sb.append(',');
                sb.append(String.format("\"%09d\"", randomNum. nextInt(10000000)));
                sb.append(',');
                int amount = randomNum. nextInt(1000);
                sb.append(String.format("%04d", amount));
                sb.append(",2023,10,31,");
                int hour = randomNum. nextInt(23);
                sb.append(hour);
                sb.append(',');
                sb.append(randomNum. nextInt(59));
                sb.append(',');
                sb.append("\"this is communication "+i+"\"");
                sb.append('\n');
                fileWriter.write(sb.toString());

                if (fromAcc.compareToIgnoreCase("\"000000000\"") == 0) {
                    //System.out.print(sb.toString());
                    if (hour == 0) totalHour += amount;
                }
                sb.delete(0,sb.length());
            }
            fileWriter.close();
            System.out.print("total amount for hour 0 for account '000000000' = "+totalHour);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
