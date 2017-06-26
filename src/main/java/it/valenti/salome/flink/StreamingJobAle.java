package main.java.it.valenti.salome.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * Created by root on 24/06/17.
 */
public class StreamingJobAle {

    public static final class LineSp implements FlatMapFunction<String,Tuple3<String,Integer,String>>{
        @Override
        public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            //out--> sid conteggio e Stringa con tutti i campi
            out.collect(new Tuple3<String,Integer,String>(tokens[0],0,tokens[1]+","+tokens[2]+","+tokens[3]+","
            +tokens[4]+","+tokens[5]));
        }
    }



    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputFile = args[0];
        String outputFile = args[1];
        DataStream<String> test = env.readTextFile(inputFile);

        DataStream<Tuple3<String,Integer,String>> stream = test.flatMap(new StreamingJobAle.LineSp())
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String,Integer,String>>() {
                    @Override
                    public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> value1, Tuple3<String, Integer, String> value2)
                            throws Exception {
                        int media =0;
                        String[] parts1 = value1.f2.split(",");
                        String[] parts2 = value2.f2.split(",");
                        if(value2!=null)
                            media = Integer.parseInt(parts1[4])+(Integer.parseInt(parts2[4])-Integer.parseInt(parts1[4]))/(value1.f1+1);

                        return new Tuple3<String,Integer,String>(value1.f0,value1.f1+1,parts1[0]+","+parts1[1]+","+parts1[2]+","
                                +parts1[3]+","+media);
                    }
                }).keyBy(0).maxBy(1);

      /*  DataStream<Tuple3<String,Integer,Query1Object>> stream = test.flatMap(new StreamingJobAle.LineSp())
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Query1Object>>() {
                    private static final long serialVersionUID = 7448326084914869599L;

                    @Override
                    public Tuple3<String, Integer, Query1Object> reduce(Tuple3<String, Integer, Query1Object> value1, Tuple3<String, Integer, Query1Object> value2)
                            throws Exception {
                        int avg = 0;
                        Query1Object app1;
                        Query1Object app2;
                        app1=value1.f2;
                        app2=value2.f2;
                        if(value2!=null)

                            avg = app1.getSpeed()+(app2.getSpeed()-app1.getSpeed())/(value1.f1+1);
                        //sid conteggio oggetto{timestamp,x,y,z,velocitàMedia}
                        return new Tuple3<String,Integer,Query1Object>(value1.f0,value1.f1+1,new Query1Object(app1.getTimestamp(),app1.getX(),app1.getY(),app1.getZ(),avg));
                    }
                }).keyBy(0).maxBy(1);*/

      //  stream.writeAsText(outputFile);
        stream.writeAsCsv(outputFile);


        env.execute("Flink Streaming Java API Skeleton");
    }
}
