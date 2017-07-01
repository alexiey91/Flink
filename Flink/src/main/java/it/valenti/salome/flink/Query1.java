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

import java.util.Date;
import java.util.Timer;


/**
 * Created by root on 27/06/17.
 */
public class Query1 {
    private final static String QUEUE_NAME = "PIO";
    static Date time = new Date();
    public static final class LineSplitter implements FlatMapFunction<String, Tuple7<String, Integer, Long, Double, Double, Double, Double>> {

        /**
         *
         */
        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(String value, Collector<Tuple7<String, Integer, Long, Double, Double, Double, Double>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");
            //out--> sid conteggio e Stringa con tutti i campi
            //double t= Math.floor(Double.parseDouble(tokens[1]));
            // long l = (long)t;
            // id-counteggio-timestamp-x-y-z-v
            out.collect(new Tuple7<String, Integer, Long, Double, Double, Double, Double>(tokens[0], 0, Long.parseLong(tokens[1]), Double.parseDouble(tokens[2]), Double.parseDouble(tokens[3]),
                    Double.parseDouble(tokens[4]), Double.parseDouble(tokens[5])));

        }
    }

    public static final class Output implements FlatMapFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>, Tuple5<Long, Long, String, Double, Double>> {

        /**
         *
         */
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
        //start-fine-id-distanza-velocità

            output.collect(new Tuple5<>(input.f2,input.f3.longValue(), input.f0, input.f5, input.f6));
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(3);

        System.out.println("Starting Test= "+time.getTime() );

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


        DataStream<Tuple7<String, Integer, Long, Double, Double, Double, Double>> ex =
                stream.flatMap(new LineSplitter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple7<String, Integer, Long, Double, Double, Double, Double> element) {
                                // System.out.println("Timestamp" + element.f2 + " ID" + element.f0);
                                return element.f2;
                            }
                        }).keyBy(0)
                        .window(TumblingEventTimeWindows.of(Time.minutes(Long.parseLong(args[0]))))
                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance ;
                                double time = value1.f2;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);
                                    distance = value1.f5 + (value2.f6 / 200);
                                    //distance = value1.f5 + Math.sqrt(Math.pow(value2.f3 - value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                    time = value2.f2;
                                } else distance = (value1.f6 / 200);
                                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                                //return new Tuple7<>(value1.f0, value1.f1 + 1, time, value1.f2.doubleValue(), value1.f4, distance, media);
                                return new Tuple7<>(value1.f0, value1.f1 + 1, value1.f2, time , value1.f4, distance, media);
                            }
                        });


        /*DataStream<Tuple7<String, Integer, Long, Double, Double, Double, Double>> timeWindow2 =
                stream.flatMap(new LineSplitter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple7<String, Integer, Long, Double, Double, Double, Double> element) {
                                // System.out.println("Timestamp" + element.f2 + " ID" + element.f0);
                                return element.f2;
                            }
                        }).keyBy(0)
                        .timeWindow(Time.minutes(5))

                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance = 0;
                                double time = value1.f2;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);
                                    distance = value1.f5 + (value2.f6 / 200);
                                    //distance = value1.f5 + Math.sqrt(Math.pow(value2.f3 - value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                    time = value2.f2;
                                } else distance = (value1.f6 / 200);
                                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                                return new Tuple7<>(value1.f0, value1.f1 + 1, value1.f2, time , value1.f4, distance, media);
                            }
                        });
*/
      /*  DataStream<Tuple7<String, Integer, Long, Double, Double, Double, Double>> timeWindow3 =
                stream.flatMap(new LineSplitter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple7<String, Integer, Long, Double, Double, Double, Double> element) {
                                // System.out.println("Timestamp" + element.f2 + " ID" + element.f0);
                                return element.f2;
                            }
                        }).keyBy(0)
                        .timeWindow(Time.minutes(68))

                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance ;
                                double time = value1.f2;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);
                                    distance = value1.f5 + (value2.f6 / 200);
                                    //distance = value1.f5 + Math.sqrt(Math.pow(value2.f3 - value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                    time = value2.f2;
                                } else distance = (value1.f6 / 200);
                                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                                return new Tuple7<>(value1.f0, value1.f1 + 1, value1.f2, time , value1.f4, distance, media);
                            }
                        });
*/


        DataStream<Tuple5<Long, Long, String, Double, Double>> query1 = ex.flatMap(new Output())
                .keyBy(2)
                .countWindow(2)
                .reduce(new ReduceFunction<Tuple5<Long, Long, String, Double, Double>>() {
                    @Override
                    public Tuple5<Long, Long, String, Double, Double> reduce(Tuple5<Long, Long, String, Double, Double> value1, Tuple5<Long, Long, String, Double, Double> value2) throws Exception {
                        double avg_speed = 0;
                        double avg_distance = 0;
                        Date tuple_date = new Date();
                        if (value2 != null) {
                            avg_speed = (value1.f4 + value2.f4) / 2;
                            avg_distance = (value1.f3 + value2.f3) / 2;
                        } else {
                            avg_distance = value1.f3;
                            avg_speed = value1.f4;
                        }
                        System.out.println("Id= "+ value2.f2+" Tuple Time "+tuple_date.getTime()+" ms");

                        return new Tuple5<>(value1.f0, value2.f1, value2.f2, avg_distance, avg_speed);
                    }
                });

       /* DataStream<Tuple5<Long, Long, String, Double, Double>> query1m5 = timeWindow2.flatMap(new Output())
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
                        return new Tuple5<>(value1.f0, value2.f1, value1.f2, avg_distance, avg_speed);
                    }
                });*/

        /*DataStream<Tuple5<Long, Long, String, Double, Double>> query1m68 = timeWindow3.flatMap(new Output())
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
                        }//tstart stop id distanza velocità
                        return new Tuple5<>(value1.f0, value2.f1, value2.f2, avg_distance, avg_speed);
                    }
                });*///.keyBy(2).flatMap(new DuplicateFilter());

        query1.writeAsText(args[1], FileSystem.WriteMode.NO_OVERWRITE);
       /* query1m5.writeAsText(args[1], FileSystem.WriteMode.NO_OVERWRITE);
        query1m68.writeAsText(args[2], FileSystem.WriteMode.NO_OVERWRITE);*/
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    //questa fuinzione dovrebbe cancellare i dublicati E funziona alla grandeeeeeee!!!!
    // link di riferimento -> https://stackoverflow.com/questions/35599069/apache-flink-0-10-how-to-get-the-first-occurence-of-a-composite-key-from-an-unbo
    public static class DuplicateFilter extends RichFlatMapFunction<Tuple5<Long, Long, String, Double, Double>, Tuple5<Long, Long, String, Double, Double>> {

        static final ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class, false);
        private ValueState<Boolean> operatorState;

        @Override
        public void open(Configuration configuration) {
            operatorState = this.getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple5<Long, Long, String, Double, Double> value, Collector<Tuple5<Long, Long, String, Double, Double>> out) throws Exception {
            if (!operatorState.value()) {
                // we haven't seen the element yet
                out.collect(value);
                // set operator state to true so that we don't emit elements with this key again
                operatorState.update(true);
            }
        }
    }

}
