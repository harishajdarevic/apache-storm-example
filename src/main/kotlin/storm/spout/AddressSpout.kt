package storm.spout

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
import java.util.*


class AddressSpout: BaseRichSpout() {

    private var collector: SpoutOutputCollector? = null
    lateinit var _rand: Random
    lateinit var redisClient: RedisClient
    lateinit var redisConnection: RedisConnection<String, String>


    override fun open(
        conf: MutableMap<String, Any>?,
        context: TopologyContext?,
        sportOutputCollector: SpoutOutputCollector?
    ) {
        collector = sportOutputCollector
        _rand = Random()

        // redis
        redisClient = RedisClient("localhost", 7777)
        redisConnection = redisClient.connect()
    }

    override fun nextTuple() {
        Utils.sleep(200)
        println("##### NEXT TUPLE ######")
        val redisQueueData = redisConnection.rpoplpush("data", "destination")

        if(redisQueueData.isNullOrEmpty()) {
            return
        } else {
            collector?.emit(Values(redisQueueData))
        }

        Thread.yield()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("address"))
    }
}