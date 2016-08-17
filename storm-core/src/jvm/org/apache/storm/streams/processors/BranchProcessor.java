package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Predicate;

import java.util.HashMap;
import java.util.Map;

public class BranchProcessor<T> extends BaseProcessor<T> {
    Map<Predicate<T>, String> predicateToStream = new HashMap<>();

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
    }

    public void addPredicate(Predicate<T> predicate, String stream) {
        predicateToStream.put(predicate, stream);
    }

    @Override
    public void execute(T input) {
        for (Map.Entry<Predicate<T>, String> entry : predicateToStream.entrySet()) {
            if (entry.getKey().test(input)) {
                context.forward(input, entry.getValue());
                break;
            }
        }
    }
}
