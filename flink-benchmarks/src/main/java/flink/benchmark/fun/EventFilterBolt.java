package flink.benchmark.fun;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;

public class EventFilterBolt implements
        FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
        return tuple.getField(4).equals("view");
    }
}