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
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

public class RuleExecutorFunction extends BroadcastProcessFunction<Message, Rule, Message> {
//    private transient MapState<Long, Set<Message>> windowState;
//
//    private MapStateDescriptor<Long, Set<Message>> windowStateDescriptor = new MapStateDescriptor<>("windowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Set<Message>>() {
//    }));

    @Override
    public void open(Configuration parameters) {
//        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }


    @Override
    public void processElement(Message message, BroadcastProcessFunction<Message, Rule, Message>.ReadOnlyContext readOnlyContext, Collector<Message> collector) throws Exception {
        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);

        for (Object ruleObject : ruleState.immutableEntries()) {
            final Rule rule = new Rule(ruleObject.toString());
            collector.collect(message.setResponseBody(rule.sqlQuery));
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Message, Rule, Message>.Context context, Collector<Message> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
