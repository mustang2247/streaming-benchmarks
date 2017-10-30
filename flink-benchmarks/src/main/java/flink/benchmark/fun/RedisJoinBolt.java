package flink.benchmark.fun;

import benchmark.common.advertising.RedisAdCampaignCache;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisJoinBolt.class);
    /**
     * redis 缓存
     */
    RedisAdCampaignCache redisAdCampaignCache;

    @Override
    public void open(Configuration parameters) {
        //initialize jedis
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        parameterTool.getRequired("jedis_server");
        LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
        this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
        this.redisAdCampaignCache.prepare();
    }

    @Override
    public void flatMap(Tuple2<String, String> input,
                        Collector<Tuple3<String, String, String>> out) throws Exception {
        String ad_id = input.getField(0);
        String campaign_id = this.redisAdCampaignCache.execute(ad_id);
        if(campaign_id == null) {
            return;
        }

        Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                campaign_id,
                (String) input.getField(0),
                (String) input.getField(1));
        out.collect(tuple);
    }
}
