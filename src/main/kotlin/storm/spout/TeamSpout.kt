package storm.spout

import com.google.gson.Gson
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
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
import storm.data.Sport
import java.util.*


class TeamSpout: BaseRichSpout() {

    private var collector: SpoutOutputCollector? = null
    lateinit var _rand: Random
    lateinit var redisClient: RedisClient
    lateinit var redisConnection: RedisConnection<String, String>
    lateinit var gson: Gson

    override fun open(
        conf: MutableMap<String, Any>?,
        context: TopologyContext?,
        sportOutputCollector: SpoutOutputCollector?
    ) {
        collector = sportOutputCollector
        _rand = Random()

        redisClient = RedisClient("localhost", 7777)
        redisConnection = redisClient.connect()
    }

    override fun ack(msgId: Any?) {

    }

    override fun nextTuple() {
        Utils.sleep(2000)
        val sportString = redisConnection.rpoplpush("data", "team")

        if(sportString.isNullOrEmpty()) {
            return
        } else {
            collector?.emit(Values(sportString))
        }

        Thread.yield()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("team"))
    }
}