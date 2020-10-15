package storm

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import storm.bolt.SaveTeamBolt
import storm.spout.TeamSpout

class CitiesTopology {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val builder = TopologyBuilder()

            builder.setSpout("address", TeamSpout(), 10)

            builder.setBolt("save-address", SaveTeamBolt(), 10).shuffleGrouping("address")

            val conf = Config()
//            conf.setNumWorkers(2000)
//            conf.setMaxTaskParallelism(2000)
            conf.setDebug(true)

            val cluster = LocalCluster()
            Thread.sleep(10000)

            cluster.submitTopology("topologija", conf, builder.createTopology())

            Thread.sleep(540000)

            cluster.killTopology("topologija")
        }
    }

}