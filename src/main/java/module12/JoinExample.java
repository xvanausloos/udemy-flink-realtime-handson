package module12;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.java.utils.ParameterTool;

public class JoinExample {

    public static void main(String[] args) throws Exception {
        //set exec env as final (non access modifier impossible to inherit or override)
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters avail in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // read person file and generate tuples out of each string read
        DataSet<Tuple2<Integer,String>> personSet = env.readTextFile(params.get("input1"))
                                        .map(new MapFunction<String, Tuple2<Integer,String>>() {
                                            @Override
                                            public Tuple2<Integer, String> map(String value) throws Exception {
                                                String[] words = value.split(",");
                                                return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                                            }
                                        });

        System.out.println(personSet.collect());

        // read location file and generate tuples ouf of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
                                            .map(new MapFunction<String, Tuple2<Integer, String>>() {
                                                @Override
                                                public Tuple2<Integer, String> map(String value) throws Exception {
                                                    String[] words = value.split(",");
                                                    return new Tuple2<Integer, String>(Integer.parseInt(words[0]),words[1]);
                                                }
                                            }
        );
        System.out.println(locationSet.collect());

        // inner join datasets on person id
        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>()
                {

                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
                    }
                });
        System.out.println("joined: ");
        System.out.println(joined.collect());
        System.out.println("*** end ***");
    }
    
}
