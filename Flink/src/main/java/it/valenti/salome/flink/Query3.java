package main.java.it.valenti.salome.flink;

import main.java.it.valenti.salome.flink.util.PositionField;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;



/**
 * Created by root on 27/06/17.
 */
public class Query3 {
    private final static String QUEUE_NAME = "CODA";
    private final static int DEFAULT_SIZE = 1;
    public static final class LineSplitter implements FlatMapFunction<String,Tuple5<Long, String, String, String,Long>> {

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(String value, Collector<Tuple5<Long, String, String, String,Long>> out) {
            /** normalizza e divide le linee lette dalla sorgente */
            String[] tokens = value.toLowerCase().split(",");
            PositionField positionField = new PositionField();


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

            String positionCell=positionField.positionCell(Double.parseDouble(tokens[2]),Double.parseDouble(tokens[3]));

            //start,stop,nomegiocatore,cella,tempocampione
            out.collect(new Tuple5<>(Long.parseLong(tokens[1]),"",name,positionCell,5L));

        }
    }


    public static final class FinalOutput implements FlatMapFunction<Tuple6<Long, String, String, String,String,Long>,String> {

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(Tuple6<Long, String, String, String,String,Long> input, Collector<String> output) throws Exception {
            /**
             * si splitta zona1:value1:zona2:value2:zona3:value3... per poter calcolare la percentuale di tempo che il giocatore passa in una cella
             **/
            String [] token= input.f4.split(Pattern.quote(":"));
            ArrayList<String> zones = new ArrayList<>();
            ArrayList<String> zValue = new ArrayList<>();
            for(int j =0;j<token.length;j++){
                if(j%2==0 && !token[j].equals(""))  zones.add(token[j]);
                else {double value = Double.parseDouble(token[j]) * 100 / input.f5.doubleValue();
                    NumberFormat format = new DecimalFormat("###.##");
                    zValue.add(format.format(value));
                }
            }

            String list="";
            for(int j =0;j<zones.size();j++)
                list+="("+zones.get(j)+":"+zValue.get(j)+"%)\n";

            Long endWindow = (input.f0/Long.parseLong(input.f1)+1)*Long.parseLong(input.f1);
            output.collect("> t_start:"+input.f0+",t_end:"+endWindow+",name:"+input.f2+","+list);
        }
    }
    public static final class MidOutput implements FlatMapFunction<Tuple5<Long, String, String, String,Long>,Tuple6<Long, String, String, String,String,Long>> {

        private static final long serialVersionUID = -6087546114124934588L;

        @Override
        public void flatMap(Tuple5<Long, String, String, String,Long> input, Collector<Tuple6<Long, String, String, String,String,Long>> output) throws Exception {
            //start,end,name,zona,zona:tempotrascorso,tempotrascorso
            output.collect(new Tuple6<>(input.f0,input.f1,input.f2,input.f3,input.f3+":"+input.f4,input.f4));
        }
    }

    public static void main(final String[] args) throws Exception {

        /** configurazione */

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int timeWindow= DEFAULT_SIZE;
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

        System.out.println("Dopo dataStrem");

        /**
         * in questa fase si camcola per ogni giocatore il tempo che ha trascorso in ogni cella
         */
        DataStream<Tuple5<Long, String, String, String,Long>> ex =
                stream.flatMap(new LineSplitter())            // timestamp-(timestamp)-id-zonaCampo-tempo
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, String, String,Long>>() {

                            @Override
                            public long extractAscendingTimestamp(Tuple5<Long, String, String, String,Long> element) {
                                return element.f0;
                            }
                        })
                        .keyBy(new KeySelector<Tuple5<Long, String, String, String,Long>, String>() {
                            //stream -> keyed stream(nome,zona)
                            public String getKey(Tuple5<Long, String, String, String,Long> keyed) { return keyed.f2+keyed.f3; }
                        })
                        .timeWindow(Time.minutes(timeWindow))
                        .reduce(new ReduceFunction<Tuple5<Long, String, String, String,Long>>() {

                            private static final long serialVersionUID = 7448326084914869599L;

                            @Override
                            public Tuple5<Long, String, String, String,Long> reduce(Tuple5<Long, String, String, String,Long> value1, Tuple5<Long, String, String, String,Long> value2)
                                    throws Exception {
                                // start,end,nome,zonaCampo,tempo cumulativo della zona di quel giocatore
                                return new Tuple5<>(value1.f0,value2.f0.toString(), value1.f2,value1.f3,value1.f4+5);
                            }
                        });
        /**
         * per ogni giocatore si calcola la percentuale di tempo trascorso in ogni cella nell'arco della finestra
         */
        DataStream<String> query = ex
                 .flatMap(new MidOutput())
                 .keyBy(2)
                 //.window(TumblingEventTimeWindows.of(Time.minutes(timeWindow)))
                 .timeWindow(Time.minutes(timeWindow))
                 .reduce(new ReduceFunction<Tuple6<Long, String, String, String,String,Long>>() {


                     private static final long serialVersionUID = 7448326084914869599L;

                     @Override
                     public Tuple6<Long, String, String, String,String,Long> reduce(Tuple6<Long, String, String, String,String,Long> value1, Tuple6<Long, String, String, String,String,Long> value2)
                             throws Exception {
                         //start,larghezzafinestra,name,zona,zona1:tempo1:zona2:tempo2...,tempoTot
                         return new Tuple6<> (value1.f0,""+EndWindow, value1.f2,value1.f3,value1.f4+":"+value2.f4 ,value1.f5 +value2.f5);

                     }
                 }).flatMap(new FinalOutput());



        query.writeAsText(args[0], FileSystem.WriteMode.NO_OVERWRITE);
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }


}
