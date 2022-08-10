import datatypes.CustomMessage;
import datatypes.Keyed;
import datatypes.Rule;
import in.org.iudx.adaptor.datatypes.Message;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;


public class RuleFunction extends KeyedBroadcastProcessFunction<String, Message, Rule, CustomMessage> {

    private transient MapState<Long, Set<Message>> windowState;

    private MapStateDescriptor<Long, Set<Message>> windowStateDescriptor = new MapStateDescriptor<>("windowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<Message>>() {
    }));

    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }

    @Override
    public void processElement(Message message, KeyedBroadcastProcessFunction<String, Message, Rule, CustomMessage>.ReadOnlyContext readOnlyContext, Collector<CustomMessage> collector) throws Exception {
        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);

        for (Object ruleObject : ruleState.immutableEntries()) {
            final Rule rule = new Rule(ruleObject.toString());
            CustomMessage customMessage = new CustomMessage();
            customMessage.setRuleString(rule.sqlQuery);
            customMessage.setSqlTimestamp(message.timestamp);
            customMessage.setResponseBody(message.body);
            customMessage.setKey(message.key);
            customMessage.setEventTimestamp(message.timestamp);
            customMessage.setEventTimeAsString(message.timestampString);
            collector.collect(customMessage);
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<String, Message, Rule, CustomMessage>.Context context, Collector<CustomMessage> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
