package module11;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class WordCount {
        public static void main(String[] args) throws Exception {

            //setup exec env
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
            DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new WordCount.Tokenizer());

            System.out.println("tokenized: " + tokenized.collect());
            // group by the tuple field "0" and sum up tuplie field 1
            DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
            System.out.println(counts.collect());
            // emit results
            if (params.has("output")){
                //counts.writeAsCsv("file:///tmp/wc.csv", "\n", " ", FileSystem.WriteMode.OVERWRITE);
                counts.writeAsCsv(params.get("output"), "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
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
