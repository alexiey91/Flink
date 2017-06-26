package main.java.it.valenti.salome.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-0.0.1-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c it.valenti.salome.flink.StreamingJob target/flink-0.0.1-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */


public class StreamingJob {
	public static final class LineSplitter implements FlatMapFunction<String, Tuple3<String, Integer, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6087546114124934588L;

		@Override
		public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split(",");

			// emit the pairs
			//key,conteggio,vel
			out.collect(new Tuple3<String, Integer, Integer>(tokens[0], 1,Integer.parseInt(tokens[1])));
			
		}
	}


	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String inputFile = args[0];
		String outputFile = args[1];
		DataStream<String> test = env.readTextFile(inputFile);





		DataStream<Tuple3<String,Integer,Integer>> ex=test.flatMap(new LineSplitter())
				.keyBy(0)
				//.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
				    /**
					*
					 */

					private static final long serialVersionUID = 7448326084914869599L;

					@Override
				    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2)
				    throws Exception {
						int media = 0;
						if(value2!=null)
							media = value1.f2+(value2.f2-value1.f2)/(value1.f1+1);
						//f0=key f1=conteggio, f2=velocità
				        return new Tuple3<String, Integer, Integer>(value1.f0,value1.f1+1,media);
				    }
				}).keyBy(0).maxBy(1);
				;
		ex.writeAsCsv(outputFile);
		//ex.print();
		
		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
