package main.java.it.valenti.salome.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Timer;


/**
 * Created by root on 27/06/17.
 */
public class Query1 {
    private final static String QUEUE_NAME = "CODA";
    static Date time = new Date();
    private final static int DEFAULT_SIZE = 1;
    private final static double Y_lenght = 67.925;


    public static final class LineSplitter implements FlatMapFunction<String, Tuple7<String, Integer, Long, Double, Double, Double, Double>> {

        /** normalizza e divide le linee lette dalla sorgente */

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(String value, Collector<Tuple7<String, Integer, Long, Double, Double, Double, Double>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");

            double adjustY= (Y_lenght/2)+Double.parseDouble(tokens[3]);
            double x;
            if(Double.parseDouble(tokens[2])<0) x=0;
            else x= Double.parseDouble(tokens[2]);

            // id-conteggio-timestamp-x-y-z-v
            out.collect(new Tuple7<>(tokens[0], 1, Long.parseLong(tokens[1]), x, adjustY,
                    0d, Double.parseDouble(tokens[5])));

        }
    }

    public static final class Output implements FlatMapFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>, Tuple5<Long, Long, String, Double, Double>> {
        /** converte id -> nome */

        private static final long serialVersionUID = -6087546114124934588L;


        @Override
        public void flatMap(Tuple7<String, Integer, Long, Double, Double, Double, Double> input, Collector<Tuple5<Long, Long, String, Double, Double>> output) throws Exception {

            switch (input.f0) {
                case "13":
                    input.f0 = "Nick Gertje";
                    break;
                case "14":
                    input.f0 = "Nick Gertje";
                    break;
                case "47":
                    input.f0 = "Dennis Dotterweich";
                    break;
                case "16":
                    input.f0 = "Dennis Dotterweich";
                    break;
                case "49":
                    input.f0 = "Niklas Waelzlein";
                    break;
                case "88":
                    input.f0 = "Niklas Waelzlein";
                    break;
                case "19":
                    input.f0 = "Wili Sommer";
                    break;
                case "52":
                    input.f0 = "Wili Sommer";
                    break;
                case "53":
                    input.f0 = "Philipp Harlass";
                    break;
                case "54":
                    input.f0 = "Philipp Harlass";
                    break;
                case "23":
                    input.f0 = "Roman Hartleb";
                    break;
                case "24":
                    input.f0 = "Roman Hartleb";
                    break;
                case "57":
                    input.f0 = "Erik Engelhardt";
                    break;
                case "58":
                    input.f0 = "Erik Engelhardt";
                    break;
                case "59":
                    input.f0 = "Sandro Schneider";
                    break;
                case "28":
                    input.f0 = "Sandro Schneider";
                    break;
                //Inizio Team2
                case "61":
                    input.f0 = "Leon Krapf";
                    break;
                case "62":
                    input.f0 = "Leon Krapf";
                    break;
                case "63":
                    input.f0 = "Kevin Baer";
                    break;
                case "64":
                    input.f0 = "Kevin Baer";
                    break;
                case "65":
                    input.f0 = "Luca Ziegler";
                    break;
                case "66":
                    input.f0 = "Luca Ziegler";
                    break;
                case "67":
                    input.f0 = "Ben Mueller";
                    break;
                case "68":
                    input.f0 = "Ben Mueller";
                    break;
                case "69":
                    input.f0 = "Vale Reitstetter";
                    break;
                case "38":
                    input.f0 = "Vale Reitstetter";
                    break;
                case "71":
                    input.f0 = "Christopher Lee";
                    break;
                case "40":
                    input.f0 = "Christopher Lee";
                    break;
                case "73":
                    input.f0 = "Leon Heinze";
                    break;
                case "74":
                    input.f0 = "Leon Heinze";
                    break;
                case "75":
                    input.f0 = "Leo Langhans";
                    break;
                case "44":
                    input.f0 = "Leo Langhans";
                    break;


            }
            // ingresso id-conteggio-start-x-y-distance-v

            //uscita start-*-name-distanza-velocità

            output.collect(new Tuple5<>(input.f2,0L, input.f0, input.f5, input.f6));
        }
    }

    public static void main(String[] args) throws Exception {
        /** configurazione */

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int timeWindow= DEFAULT_SIZE;

        System.out.println("Starting Test= "+time.getTime() );
        if(args.length != 4)
            System.out.println("Usage: .\\bin\\flink run -c main.java.it.valenti.salome.flink.Query3 .\\flink.jar <fileOut,sizeWindow in minutes, parallelism,source(0= flink/1=file)>");

        DataStream<String> stream;

        if(args[3].equals("0")) {
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
            stream = env
                    .addSource(new RMQSource<String>(
                            connectionConfig,            // config for the RabbitMQ connection
                            QUEUE_NAME,                 // name of the RabbitMQ queue to consume
                            true,   // use correlation ids; can be false if only at-least-once is required
                            new SimpleStringSchema()));   // deserialization schema to turn messages into Java objects
        }
        else
            stream = env.readTextFile("..\\FilterFile.txt");

        if(args[1]!=null)
            timeWindow = Integer.parseInt(args[1]);
        if(args[2]!=null)
            env.setParallelism(Integer.parseInt(args[2]));
        final long EndWindow = timeWindow*60000;

        /** si calcola la distanza percorsa da ogni giocatore e la velocità media all'interno della finestra */

        DataStream<Tuple7<String, Integer, Long, Double, Double, Double, Double>> ex =
                stream.flatMap(new LineSplitter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple7<String, Integer, Long, Double, Double, Double, Double> element) {
                                return element.f2;
                            }
                        }).keyBy(0)
                        .window(TumblingEventTimeWindows.of(Time.minutes(Long.parseLong(args[1]))))
                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance=value1.f5 ;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);
                                    distance = value1.f5 + Math.sqrt(Math.pow(value2.f3-value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                }
                                // id-conteggio-start-x-y-distance-v

                                return new Tuple7<>(value1.f0, value1.f1 + 1, value1.f2, value2.f3, value2.f4, distance, media);
                            }
                        });




        /** si effettua la media tra i sensori associati a ciascun giocatore */

        DataStream<String> query1 = ex.flatMap(new Output())
                .keyBy(2)
                .countWindow(2)
                .reduce(new ReduceFunction<Tuple5<Long, Long, String, Double, Double>>() {
                    @Override
                    public Tuple5<Long, Long, String, Double, Double> reduce(Tuple5<Long, Long, String, Double, Double> value1, Tuple5<Long, Long, String, Double, Double> value2) throws Exception {
                        double avg_speed = 0;
                        double avg_distance = 0;
                        if (value2 != null) {
                            avg_speed = (value1.f4 + value2.f4) / 2;
                            avg_distance = (value1.f3 + value2.f3) / 2;
                        } else {
                            avg_distance = value1.f3;
                            avg_speed = value1.f4;
                        }
                        //System.out.println("Id= "+ value2.f2+" Tuple Time "+tuple_date.getTime()+" ms");
                        Long endWindow = (value1.f0/EndWindow+1)*EndWindow;

                        return new Tuple5<>(value1.f0, endWindow, value2.f2, avg_distance, avg_speed);
                    }
                }).flatMap(new FlatMapFunction<Tuple5<Long, Long, String, Double, Double>, String>() {
                    @Override
                    public void flatMap(Tuple5<Long, Long, String, Double, Double> input, Collector<String> collector) throws Exception {
                        NumberFormat format = new DecimalFormat("###.##");
                        collector.collect(("t_start: "+input.f0+", t_end: "+input.f1+", id: "+ input.f2+", distance: "+format.format(input.f3)+", speed: "+format.format(input.f4)));
                    }
                });



        query1.writeAsText(args[0], FileSystem.WriteMode.NO_OVERWRITE);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}
