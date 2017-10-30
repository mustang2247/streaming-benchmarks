package flink.benchmark.fun;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

public class DeserializeBolt implements
        FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

    @Override
    public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
            throws Exception {
        JSONObject obj = new JSONObject(input);
        Tuple7<String, String, String, String, String, String, String> tuple =
                new Tuple7<String, String, String, String, String, String, String>(
                        obj.getString("user_id"),
                        obj.getString("page_id"),
                        obj.getString("ad_id"),
                        obj.getString("ad_type"),
                        obj.getString("event_type"),
                        obj.getString("event_time"),
                        obj.getString("ip_address"));
        out.collect(tuple);
    }
}
