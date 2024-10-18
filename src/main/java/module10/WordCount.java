package module10;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.api.java.tuple.Tuple2;
import scala.Int;

public class WordCount {
    public static void main(String[] args) throws Exception {

        //setup exec env
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        //read the text file in Flink batch mode using Dataset
        DataSet<String> text = env.readTextFile(params.get("input"));

        //filter all the names starting by N
        DataSet<String> filtered = text.filter(new FilterFunction<String>() {

            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("N");
            }
        });

        // return a tuple of (name, 1)
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        System.out.println("tokenized: " + tokenized.collect());
        // group by the tuple field "0" and sum up tuplie field 1
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        System.out.println(counts.collect());
        // emit results
        if (params.has("output")){
            //counts.writeAsCsv("file:///tmp/wc.csv", "\n", " ", FileSystem.WriteMode.OVERWRITE);
            // setParallelism 1 for getting one CSV file
            counts.writeAsCsv("file:///tmp/wc.csv", "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            // exec the program
            env.execute("word count example");
        }
        
    }

    public static final class Tokenizer implements MapFunction< String, Tuple2 < String, Integer >> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            return new Tuple2<String, Integer>(value, 1);
        }
    }
}
