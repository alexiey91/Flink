package main.java.it.valenti.salome.flink;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Created by root on 26/06/17.
 */
public class StreamingJobRabbit {
    private final static String QUEUE_NAME = "PIO";
    private  static final long Start_match = 10753295594424116L;
    private static final long Starting = 10629342490369879L;

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
      //  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        factory.setUsername("guest");
       factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);


        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
                String[] parts = sCurrentLine.split(",");

                parts[1] = "" + ((Long.parseLong(parts[1]) - Start_match));
                parts[1] = "" + (Math.round(Long.parseLong(parts[1]) / 1000000000));
                parts[2] = ""+((Double.parseDouble(parts[2]) / 1000));
                parts[3] = ""+((Double.parseDouble(parts[3]) / 1000));
                parts[4] = ""+((Double.parseDouble(parts[4]) / 1000));
                parts[5] = ""+((Double.parseDouble(parts[5]) / 1000000));
                String s = "-"+parts[0]+"-";
                String test = "-4-8-10-12-97-98-99-100-105-106-";
                if (Long.parseLong(parts[1]) < 0 || test.contains(s)) {
                    //System.out.println("long continue = " + Long.parseLong(parts[1]));
                    continue;
                }
                else {
                    String line = parts[0];
                    for (int i = 0; i < parts.length - 2 ; i++) {
                        line += "," + parts[i + 1];
                    }
                    channel.basicPublish("", QUEUE_NAME, null, line.getBytes("UTF-8"));
                    //System.out.println(" [x] Sent '" + line + "'");

                    //Eseguere Lettura & Query1
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }



       // env.execute();
        channel.close();
        connection.close();




    }
}
