import datatypes.Rule;
import in.org.iudx.adaptor.codegen.ApiConfig;
import in.org.iudx.adaptor.datatypes.Message;
import in.org.iudx.adaptor.process.GenericProcessFunction;
import in.org.iudx.adaptor.process.JoltTransformer;
import in.org.iudx.adaptor.process.TimeBasedDeduplicator;
import in.org.iudx.adaptor.source.HttpSource;
import in.org.iudx.adaptor.source.JsonPathParser;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.HashMap;

public class Main {

    private static String RMQ_URI = "amqp://guest:guest@rmq:5672";


    private static String RMQ_SQL_QUEUE_NAME = "rules";

    public static void main(String[] args) {
        HashMap<String, String> propertyMap = new HashMap<>();
        propertyMap.put("appName", "test");
        ParameterTool parameters = ParameterTool.fromMap(propertyMap);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 100 * 1000);


        // RMQ Source for SQL Queries
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder().setUri(RMQ_URI).build();
        final DataStream<String> sqlSourceStream = env.addSource(new RMQSource<String>(connectionConfig, RMQ_SQL_QUEUE_NAME, new SimpleStringSchema())).setParallelism(1);

        DataStream<Rule> rules = sqlSourceStream.flatMap(new RuleSourceDeSerializer()).name("Rules Source").setParallelism(1);


        BroadcastStream<Rule> ruleBroadcastStream = rules.broadcast(Descriptors.ruleMapStateDescriptor);


        // Http Source for data

        ApiConfig apiConfig = new ApiConfig().setUrl("http://13.232.120.105:30002/simpleA").setRequestType("GET").setPollingInterval(1000);
        String parseSpec = "{\"messageContainer\":\"single\",\"timestampPath\":\"$.time\",\"keyPath\":\"$.deviceId\",\"type\":\"json\"}";
        JsonPathParser<Message> parser = new JsonPathParser<>(parseSpec);
        DataStreamSource<Message> so = env.addSource(new HttpSource<>(apiConfig, parser));
        TimeBasedDeduplicator dedup = new TimeBasedDeduplicator();
        String transformSpec = "{\"type\":\"jolt\",\"joltSpec\":[{\"operation\":\"shift\",\"spec\":{\"k1\":\"k1\",\"time\":\"observationDateTime\",\"deviceId\":\"id\"}},{\"operation\":\"modify-overwrite-beta\",\"spec\":{\"id\":\"=concat('datakaveri.org/123/', id)\"}}]}";
        JoltTransformer trans = new JoltTransformer(transformSpec);

//        DataStream<Message> dataSource = so.keyBy((Message msg) -> msg.key).process(new GenericProcessFunction(trans, dedup));

        SingleOutputStreamOperator<Message> out = so
                .keyBy((Message msg) -> msg.key)
                .process(new GenericProcessFunction(trans, dedup))
                .uid("GenericProcessFunction")
                .name("Generic Process Function")
//                .keyBy((Message msg) -> msg.key)
                .connect(ruleBroadcastStream)
                .process(new RuleFunction())
                .uid("RuleSourceFunction")
                .name("Rule Source Function")
//                .keyBy((Message msg) -> msg.key)
                .connect(ruleBroadcastStream)
                .process(new RuleExecutorFunction())
                .uid("RuleExecutorFunction")
                .name("Rule Executor Function")
                .setParallelism(1);



        out.addSink(new PrintSinkFunction<>());
        try {
            env.getConfig().setGlobalJobParameters(parameters);
            env.execute();
        } catch (Exception e) {
        }
    }

    public static class Descriptors {
        public static final MapStateDescriptor<String, Rule> ruleMapStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Rule.class));
    }
}
