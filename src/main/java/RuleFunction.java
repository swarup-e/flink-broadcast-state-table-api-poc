import datatypes.Keyed;
import datatypes.Rule;
import in.org.iudx.adaptor.datatypes.Message;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


public class RuleFunction extends BroadcastProcessFunction<Message, Rule, Keyed<Message, String>> {

    @Override
    public void processElement(Message message, BroadcastProcessFunction<Message, Rule, Keyed<Message, String>>.ReadOnlyContext readOnlyContext, Collector<Keyed<Message, String>> collector) throws Exception {
        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);

        for (Object ruleObject : ruleState.immutableEntries()) {
            final Rule rule = new Rule(ruleObject.toString());
            collector.collect(new Keyed<Message, String>(message, message.key));
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Message, Rule, Keyed<Message, String>>.Context context, Collector<Keyed<Message, String>> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
