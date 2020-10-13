package storm.spout

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter
import com.lambdaworks.redis.pubsub.RedisPubSubConnection
import com.lambdaworks.redis.pubsub.RedisPubSubListener
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
    lateinit var redisPubSubConnection: RedisPubSubConnection<String, String>
    lateinit var redisClient: RedisClient



    override fun open(
        conf: MutableMap<String, Any>?,
        context: TopologyContext?,
        sportOutputCollector: SpoutOutputCollector?
    ) {
        collector = sportOutputCollector
        _rand = Random()
    }


    override fun nextTuple() {
        Utils.sleep(15000)

        redisClient = RedisClient("localhost", 7777)
        val redisPubSubConnection = redisClient.connectPubSub()

        val listener: RedisPubSubListener<String, String> = object : RedisPubSubAdapter<String, String>() {
            override fun message(channel: String, message: String) {
                collector?.emit(Values(message))
            }
        }

        redisPubSubConnection.addListener(listener)
        redisPubSubConnection.subscribe("data")
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("address"))
    }
}