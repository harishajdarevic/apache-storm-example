package storm.bolt

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

class SaveAddressBolt: BaseRichBolt() {

    private var collector: OutputCollector? = null

    override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?, outputCollector: OutputCollector) {
        collector = outputCollector
    }

    override fun execute(tuple: Tuple?) {
        println("Execute bolt:  ${tuple?.getString(0)}")
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
    }
}