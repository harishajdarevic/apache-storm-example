package storm.bolt

import com.google.gson.Gson
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils


class SaveTeamBolt: BaseRichBolt() {

    private var collector: OutputCollector? = null


    override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?, outputCollector: OutputCollector) {
        collector = outputCollector
    }

    override fun execute(tuple: Tuple?) {
//        Utils.sleep(5000)
        collector?.ack(tuple)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
    }
}