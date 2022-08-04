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


public class RuleFunction extends KeyedBroadcastProcessFunction<String, Message, Rule, Message> {

//    @Override
//    public void processElement(Message message, BroadcastProcessFunction<Message, Rule, Keyed<Message, String>>.ReadOnlyContext readOnlyContext, Collector<Keyed<Message, String>> collector) throws Exception {
//        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
//
//        for (Object ruleObject : ruleState.immutableEntries()) {
//            final Rule rule = new Rule(ruleObject.toString());
//            collector.collect(new Keyed<Message, String>(message, message.key));
//        }
//    }
//
//    @Override
//    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Message, Rule, Keyed<Message, String>>.Context context, Collector<Keyed<Message, String>> collector) throws Exception {
//        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
//        broadcastState.put(rule.sqlQuery, rule);
//    }

    private transient MapState<Long, Set<Message>> windowState;

    private MapStateDescriptor<Long, Set<Message>> windowStateDescriptor = new MapStateDescriptor<>("windowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<Message>>() {
    }));

    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }


    @Override
    public void processElement(Message message, KeyedBroadcastProcessFunction<String, Message, Rule, Message>.ReadOnlyContext readOnlyContext, Collector<Message> collector) throws Exception {
        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);

        for (Object ruleObject : ruleState.immutableEntries()) {
            final Rule rule = new Rule(ruleObject.toString());
            collector.collect(message);
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<String, Message, Rule, Message>.Context context, Collector<Message> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
