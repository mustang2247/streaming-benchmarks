package flink.benchmark.fun;

import benchmark.common.advertising.CampaignProcessorCommon;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(CampaignProcessor.class);

    CampaignProcessorCommon campaignProcessorCommon;

    @Override
    public void open(Configuration parameters) {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        parameterTool.getRequired("jedis_server");
        LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));

        this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"));
        this.campaignProcessorCommon.prepare();
    }

    @Override
    public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

        String campaign_id = tuple.getField(0);
        String event_time =  tuple.getField(2);
        this.campaignProcessorCommon.execute(campaign_id, event_time);
    }
}