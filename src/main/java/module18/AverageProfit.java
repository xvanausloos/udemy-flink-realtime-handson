package module18;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {

    // this time we implement fold operation instread of reduce operation.

    public static void main(String[] args) throws Exception {
        //set up streaming env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.readTextFile("resources/avg");
        // DataStreamSource<String> data = env.readTextFile("/Users/xaviervanausloos/temp/avg");

        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());
        //mapped.print();

        //group by month
        DataStream<Tuple4<String, String, Integer, Integer>> folded = mapped.keyBy(0).fold(
                new Tuple4<String, String, Integer, Integer> ("","",0,0), new FoldFunction1());

        // month avg profit
        DataStream<Tuple2<String, Double>> profitPerMonth = folded.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> input) throws Exception {
                return new Tuple2<String, Double>(input.f0, new Double((input.f2 * 1.0) / input.f3));
            }
        });
        System.out.println("---- profit per month ---");
        profitPerMonth.print();

        env.execute("average profit fold");

        System.out.println("*** end average profit ***");
        
    }

    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1);
        }                                                            //    June    Category5      Bat                      12
    }


    // *************************************************************************
    // USER FUNCTIONS                                                                                  // pre_result  = Category4,Perfume,22,2
    // *************************************************************************

    public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_result)
        {
            return new Tuple5<String, String, String, Integer, Integer>(current.f0,current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
        }
    }

    private static class FoldFunction1 implements FoldFunction<Tuple5<String, String, String, Integer, Integer>, Tuple4<String, String, Integer, Integer>> {

        @Override
        public Tuple4<String, String, Integer, Integer> fold(Tuple4<String, String, Integer, Integer> defaultIn,
                                         Tuple5<String, String, String, Integer, Integer> curr) throws Exception {
            defaultIn.f0 = curr.f0;
            defaultIn.f1 = curr.f1;
            defaultIn.f2 += curr.f3;
            defaultIn.f3 += curr.f4;
            return defaultIn;
        }
    }
}
