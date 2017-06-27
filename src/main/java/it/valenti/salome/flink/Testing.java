package main.java.it.valenti.salome.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Created by root on 27/06/17.
 */
public class Testing {
    private final static String QUEUE_NAME = "PIO";

    public static final class LineSplitter implements FlatMapFunction<String, Tuple4<String,Integer,Long,String>> {

        /**
         *
         */
        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(String value, Collector<Tuple4<String,Integer,Long,String>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");
            //out--> sid conteggio e Stringa con tutti i campi
            //double t= Math.floor(Double.parseDouble(tokens[1]));
           // long l = (long)t;

            out.collect(new Tuple4<String,Integer,Long,String>(tokens[0],0,Long.parseLong(tokens[1]),tokens[2]+","+tokens[3]+","
                    +tokens[4]+","+tokens[5]));

        }
    }


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        org.apache.flink.streaming.api.windowing.time.Time  window1 =  org.apache.flink.streaming.api.windowing.time.Time.seconds(2);

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .setConnectionTimeout(5000)
                // .setTopologyRecoveryEnabled(false)
                .build();
        System.out.println("Prima di dataStrem");
        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        QUEUE_NAME,                 // name of the RabbitMQ queue to consume
                        true,   // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()));   // deserialization schema to turn messages into Java objects


        System.out.println("Dopo dataStrem");



        DataStream<Tuple4<String,Integer,Long,String>> ex=
                stream.flatMap(new LineSplitter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String,Integer,Long,String>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple4<String,Integer,Long,String> element) {
                              System.out.println("Timestamp"+ element.f2+" ID"+ element.f0);
                                return element.f2;
                            }
                        }).keyBy(0)
                        .timeWindow(Time.milliseconds(500))
                        .reduce(new ReduceFunction<Tuple4<String,Integer,Long,String>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple4<String,Integer,Long,String> reduce(Tuple4<String,Integer,Long,String> value1, Tuple4<String,Integer,Long,String> value2)
                                    throws Exception {
                                int media = 0;


                                String[] parts1 = value1.f3.split(",");
                                String[] parts2 = value2.f3.split(",");
                                if (value2 != null)
                                    media = Integer.parseInt(parts1[3]) + (Integer.parseInt(parts2[3]) - Integer.parseInt(parts1[3])) / (value1.f1 + 1);
                                System.out.println("media"+media);
                                return new Tuple4<String, Integer, Long,String>(value1.f0, value1.f1 + 1, value1.f2,parts1[0] + "," + parts1[1] + "," + parts1[2] +"," + media);
                            }
                        });//.keyBy(0).maxBy(1);


        //ex.writeAsCsv(outputFile);
        ex.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}
