package storm.spout

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values

class TeamSpout(private val reliable: Boolean = false): BaseRichSpout() {

    private var collector: SpoutOutputCollector? = null
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
    }

    override fun ack(team: Any?) {
        redisConnection.rpush("team", team.toString())
    }

    override fun fail(team: Any?) {
        redisConnection.rpush("data", team.toString())
    }

    override fun nextTuple() {

        val sportString = redisConnection.rpop( "data")

        if (sportString.isNullOrEmpty()) {
            Thread.yield()
            return
        } else {
            if (reliable) {
                collector?.emit(Values(sportString), sportString)
            } else {
                collector?.emit(Values(sportString))
            }

        }

        Thread.yield()
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("team"))
    }
}