import datatypes.Rule;
import in.org.iudx.adaptor.datatypes.Message;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class RuleExecutorFunction extends BroadcastProcessFunction<Message, Rule, Keyed<String>> {
    @Override
    public void open(Configuration configuration) {
    }

    @Override
    public void processElement(Message message, BroadcastProcessFunction<Message, Rule, Keyed<String>>.ReadOnlyContext readOnlyContext, Collector<Keyed<String>> collector) throws Exception {
        ReadOnlyBroadcastState ruleState = readOnlyContext.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);

        for (Object ruleObject : ruleState.immutableEntries()) {
            final Rule rule = new Rule(ruleObject.toString());
            collector.collect(new Keyed<>() {
                @Override
                public String getKey() {
                    return message.key + rule.sqlQuery;
                }
            });
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Message, Rule, Keyed<String>>.Context context, Collector<Keyed<String>> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
