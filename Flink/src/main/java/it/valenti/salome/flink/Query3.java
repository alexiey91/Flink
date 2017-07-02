package main.java.it.valenti.salome.flink;

import main.java.it.valenti.salome.flink.util.TupleTreeSet;
import main.java.it.valenti.salome.flink.util.ZoneMap;
import main.java.it.valenti.salome.flink.util.ZoneSet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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

import java.util.Set;
import java.util.TreeSet;


/**
 * Created by root on 27/06/17.
 */
public class Query3 {
    private final static String QUEUE_NAME = "PIO";
    private final static int DEFAULT_SIZE = 1;
    public static final class LineSplitter implements FlatMapFunction<String,Tuple5<Long, String, String, String,Long>> {

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(String value, Collector<Tuple5<Long, String, String, String,Long>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");
            //out--> sid conteggio e Stringa con tutti i campi
            //double t= Math.floor(Double.parseDouble(tokens[1]));
            // long l = (long)t;
            /** qui bisogna convertire le coordinate in zona**/





            String name="";
            switch (tokens[0]) {
                case "13":
                    name = "Nick Gertje";
                    break;
                case "14":
                    name = "Nick Gertje";
                    break;
                case "47":
                    name = "Dennis Dotterweich";
                    break;
                case "16":
                    name = "Dennis Dotterweich";
                    break;
                case "49":
                    name = "Niklas Waelzlein";
                    break;
                case "88":
                    name = "Niklas Waelzlein";
                    break;
                case "19":
                    name = "Wili Sommer";
                    break;
                case "52":
                    name = "Wili Sommer";
                    break;
                case "53":
                    name = "Philipp Harlass";
                    break;
                case "54":
                    name = "Philipp Harlass";
                    break;
                case "23":
                    name = "Roman Hartleb";
                    break;
                case "24":
                    name = "Roman Hartleb";
                    break;
                case "57":
                    name = "Erik Engelhardt";
                    break;
                case "58":
                    name = "Erik Engelhardt";
                    break;
                case "59":
                    name = "Sandro Schneider";
                    break;
                case "28":
                    name = "Sandro Schneider";
                    break;
                //Inizio Team2
                case "61":
                    name = "Leon Krapf";
                    break;
                case "62":
                    name = "Leon Krapf";
                    break;
                case "63":
                    name = "Kevin Baer";
                    break;
                case "64":
                    name = "Kevin Baer";
                    break;
                case "65":
                    name = "Luca Ziegler";
                    break;
                case "66":
                    name = "Luca Ziegler";
                    break;
                case "67":
                    name = "Ben Mueller";
                    break;
                case "68":
                    name = "Ben Mueller";
                    break;
                case "69":
                    name = "Vale Reitstetter";
                    break;
                case "38":
                    name = "Vale Reitstetter";
                    break;
                case "71":
                    name = "Christopher Lee";
                    break;
                case "40":
                    name = "Christopher Lee";
                    break;
                case "73":
                    name = "Leon Heinze";
                    break;
                case "74":
                    name = "Leon Heinze";
                    break;
                case "75":
                    name = "Leo Langhans";
                    break;
                case "44":
                    name = "Leo Langhans";
                    break;

            }

            String tm1;

            if(name.contains("v"))
                tm1="zona";//temp.add(new ZoneSet("zona",50));
            else if(name.contains("L"))
                tm1="zonaA";//temp.add(new ZoneSet("zonaA",50));
            else if(name.contains("Z"))
                tm1="zonaB";//temp.add(new ZoneSet("zonaB",50));
            else if(name.contains("W"))
                tm1="zonaC";//temp.add(new ZoneSet("zonaC",50));
            else tm1 = "assfa";
            // timestamp-(timestamp)-id-zonaCampo-tempo

            out.collect(new Tuple5<>(Long.parseLong(tokens[1]),"",name,tm1,50L));

        }
    }


    public static final class FinalOutput implements FlatMapFunction<Tuple5<Long, String, String, String,Long>,String> {

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(Tuple5<Long, String, String, String,Long> input, Collector<String> output) throws Exception {
            output.collect("<t_start:"+input.f0+",t_end:"+input.f1+",name:"+input.f2+","+input.f3);
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(3);
        int timeWindow= DEFAULT_SIZE;

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

        if(args[1]!=null)
            timeWindow = Integer.parseInt(args[1]);
        if(args[2]!=null)
            env.setParallelism(Integer.parseInt(args[2]));

        System.out.println("Dopo dataStrem");


        DataStream<Tuple5<Long, String, String, String,Long>> ex =
                stream.flatMap(new LineSplitter())            // timestamp-(timestamp)-id-zonaCampo-tempo
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, String, String,Long>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple5<Long, String, String, String,Long> element) {
                                return element.f0;
                            }
                        }).keyBy(2,3)
                        .window(TumblingEventTimeWindows.of(Time.minutes(timeWindow)))
                        .reduce(new ReduceFunction<Tuple5<Long, String, String, String,Long>>() {


                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple5<Long, String, String, String,Long> reduce(Tuple5<Long, String, String, String,Long> value1, Tuple5<Long, String, String, String,Long> value2)
                                    throws Exception {
                                // timestamp-timestamp2-id-zonaCampo-tempoIncr
                                return new Tuple5<>(value1.f0,value2.f0.toString(), value1.f2,value1.f3,value1.f4+value2.f4 );
                            }
                        });

        DataStream<String> query = ex
                .keyBy(2)
                .reduce(new ReduceFunction<Tuple5<Long, String, String, String, Long>>() {
                    long windowActual = 1;
                    ZoneMap map = new ZoneMap();
                    //Set<ZoneSet> set = new TreeSet<>();

                    @Override
                    public Tuple5<Long, String, String, String, Long> reduce(Tuple5<Long, String, String, String, Long> value1, Tuple5<Long, String, String, String, Long> value2)
                            throws Exception {
                       // System.out.print("VALUE1" + value1.f0 + "," + value1.f1 + "," + value1.f2 + "," + value1.f3 + "," + value1.f4);
                        System.out.print("\nVALUE2" + value2.f0 + "," + value2.f1 + "," + value2.f2 + "," + value2.f3 + "," + value2.f4);
                       // if(value1.f1.equals("") || value1.f1.isEmpty()) {

                            if (Long.parseLong(value1.f1) - 60000*windowActual > 0) { //caso nuova finestra
                                map = new ZoneMap();
                                windowActual += 1;
                                System.out.print("RECALL_VALUE1 " + value1.f0 + "," + value1.f1 + "," + value1.f2 + "," + value1.f3 + "," + value1.f4);
                           //     System.out.print("\nRECALL_VALUE2 " + value2.f0 + "," + value2.f1 + "," + value2.f2 + "," + value2.f3 + "," + value2.f4);
                                // set.add(new ZoneSet(value1.f3,value1.f4));
                            }
                        if(value2 == null || value2.f1.equals("") || value2.f1.isEmpty())
                            map.updateMap(value1.f2, value1.f3, value1.f4);
                        else
                            map.updateMap(value1.f2, value2.f3, value2.f4);

                        //set.add(new ZoneSet(value2.f3,value2.f4));
                        String list = ":";
                        //for (ZoneSet k : set)

                        for (ZoneSet k : map.getMap().get(value1.f2))
                            list += "<" + k.getId() + "," +k.getCnt()+ ">\n";
                        System.out.println(value1.f0+"list[" + windowActual + "]" + list);

                        if(value2.f1.equals("") || value2.f1.isEmpty())
                            return new Tuple5<>(value1.f0, value1.f1, value1.f2, list, 0L);
                        else
                            return new Tuple5<>(value1.f0, value2.f1, value1.f2, list, 0L);
                    }
                }).flatMap(new FinalOutput());


        query.writeAsText(args[0], FileSystem.WriteMode.NO_OVERWRITE);
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }


}
