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

    override fun activate() {
        redisClient = RedisClient("localhost", 7777)
        redisConnection = redisClient.connect()
    }

    override fun open(
        conf: MutableMap<String, Any>?,
        context: TopologyContext?,
        sportOutputCollector: SpoutOutputCollector?
    ) {
        collector = sportOutputCollector
        _rand = Random()
    }

    override fun ack(team: Any?) {
//        Utils.sleep(500)
        println("tupple successfully processed")
        redisConnection.rpush("team", team.toString())
    }

    override fun fail(msgId: Any?) {
        println("failed")
    }

    override fun nextTuple() {
        println("nextTuple spout...")
//        Utils.sleep(3000)
//        val sportString = redisConnection.rpoplpush("data", "team")
        val sportString = redisConnection.rpop("data")
        println("nextTuple data ${sportString}")
        if (sportString.isNullOrEmpty()) {

            Thread.yield()
            return
        } else {
//            collector?.emit(Values(sportString))
            // Anchored ( ack )
            collector?.emit(Values(sportString), sportString)
        }

        Thread.yield()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("team"))
    }
}