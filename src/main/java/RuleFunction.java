import datatypes.Rule;
import in.org.iudx.adaptor.datatypes.Message;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class RuleFunction extends BroadcastProcessFunction<Message, Rule, Message> {

    @Override
    public void processElement(Message message, BroadcastProcessFunction<Message, Rule, Message>.ReadOnlyContext readOnlyContext, Collector<Message> collector) throws Exception {
        collector.collect(message);
    }

    @Override
    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Message, Rule, Message>.Context context, Collector<Message> collector) throws Exception {
        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(Main.Descriptors.ruleMapStateDescriptor);
        broadcastState.put(rule.sqlQuery, rule);
    }
}
