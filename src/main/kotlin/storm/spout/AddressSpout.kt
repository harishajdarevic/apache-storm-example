package storm.spout

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values
import org.apache.storm.utils.Utils
import java.util.*

class AddressSpout: BaseRichSpout() {

    private var collector: SpoutOutputCollector? = null
    lateinit var _rand: Random
    lateinit var redis: RedisConnection<String, String>
    lateinit var redisClient: RedisClient

    override fun open(
        conf: MutableMap<String, Any>?,
        context: TopologyContext?,
        sportOutputCollector: SpoutOutputCollector?
    ) {
        collector = sportOutputCollector
        _rand = Random()

        redisClient = RedisClient("localhost", 7777)

        redis = redisClient.connect()
    }


    override fun nextTuple() {
        Utils.sleep(100)
        
        val sentences = arrayOf(
            "Recenica 1",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        )
        val sentence = sentences[_rand.nextInt(sentences.size)]
        collector?.emit(Values(sentence))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("address"))
    }
}