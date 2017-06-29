package main.java.it.valenti.salome.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
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

            out.collect(new Tuple7<String, Integer, Long, Double, Double, Double, Double>(tokens[0], 0, Long.parseLong(tokens[1]), Double.parseDouble(tokens[2]), Double.parseDouble(tokens[3]),
                    Double.parseDouble(tokens[4]), Double.parseDouble(tokens[5])));

        }
    }


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        org.apache.flink.streaming.api.windowing.time.Time window1 = org.apache.flink.streaming.api.windowing.time.Time.seconds(2);

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
                        .timeWindow(Time.minutes(1))

                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance = 0;
                                long time = value1.f2;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);
                                    distance = value1.f5 + (value2.f6/200);
                                    //distance = value1.f5 + Math.sqrt(Math.pow(value2.f3 - value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                    time = value2.f2;
                                }
                                else distance = (value1.f6/200);
                                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                                return new Tuple7<String, Integer, Long, Double, Double, Double, Double>(value1.f0, value1.f1 + 1, time, value1.f3, value1.f4, distance, media);
                            }
                        }).keyBy(0).flatMap(new DuplicateFilter());
                       /*

                       Per Paolo -> Ho commentato questo codice perchè il countWIndows significa che la finesta
                       da 500 ms riceve 5 tuple alla volta e non centra nulla con la logica dei 5 minuti
                       .countWindow(5)//.keyBy(0).maxBy(1)
                        .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                                    throws Exception {
                                double media = 0;
                                double distance = 0;
                                if (value2 != null) {
                                    media = value1.f6 + (value2.f6 - value1.f6) / (value1.f1 + 1);

                                    distance = value1.f5 + Math.sqrt(Math.pow(value2.f3 - value1.f3, 2) + Math.pow(value2.f4 - value1.f4, 2));
                                }
                                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                                return new Tuple7<String, Integer, Long, Double, Double, Double, Double>(value1.f0, value1.f1 + 1, value1.f2, value1.f3, value1.f4, distance, media);
                            }
                        }).keyBy(0).max(1)*/
               /* .reduce(new ReduceFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>>() {


            private static final long serialVersionUID = 7448326084914869599L;

            @Override
            public Tuple7<String, Integer, Long, Double, Double, Double, Double> reduce(Tuple7<String, Integer, Long, Double, Double, Double, Double> value1, Tuple7<String, Integer, Long, Double, Double, Double, Double> value2)
                    throws Exception {

                if(value2!=null){
                    if (value1.f1 > value2.f2)
                        return new Tuple7<String, Integer, Long, Double, Double, Double, Double>("",0,0L,0D,0D,0D,0D);
                    else   return new Tuple7<String, Integer, Long, Double, Double, Double, Double>(value2.f0, value2.f1 , value2.f2, value2.f3, value2.f4, value2.f5, value2.f5);
                }

                // System.out.println("media"+media+" distance= "+distance+" m"+" difference x ="+(value2.f3-value1.f3)+" difference y ="+(value2.f4-value1.f4));
                else   return new Tuple7<String, Integer, Long, Double, Double, Double, Double>(value1.f0, value1.f1 , value1.f2, value1.f3, value1.f4, value1.f5, value1.f5);
            }
        })*/;


        ex.writeAsText(args[0]);
        //ex.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    //questa fuinzione dovrebbe cancellare i dublicati E funziona alla grandeeeeeee!!!!
    // link di riferimento -> https://stackoverflow.com/questions/35599069/apache-flink-0-10-how-to-get-the-first-occurence-of-a-composite-key-from-an-unbo
    public static class DuplicateFilter extends RichFlatMapFunction<Tuple7<String, Integer, Long, Double, Double, Double, Double>, Tuple7<String, Integer, Long, Double, Double, Double, Double>> {

        static final ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class, false);
        private ValueState<Boolean> operatorState;

        @Override
        public void open(Configuration configuration) {
            operatorState = this.getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple7<String, Integer, Long, Double, Double, Double, Double> value, Collector<Tuple7<String, Integer, Long, Double, Double, Double, Double>> out) throws Exception {
            if (!operatorState.value()) {
                // we haven't seen the element yet
                out.collect(value);
                // set operator state to true so that we don't emit elements with this key again
                operatorState.update(true);
            }
        }
    }

}
