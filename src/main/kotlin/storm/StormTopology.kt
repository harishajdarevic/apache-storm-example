package storm

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import storm.bolt.SaveTeamBolt
import storm.spout.TeamSpout

class StormTopology {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val builder = TopologyBuilder()

//            builder.setSpout("team", TeamSpout(), 10)
            builder.setSpout("team", TeamSpout(), 20)

//            builder.setBolt("save-team", SaveTeamBolt(), 10).shuffleGrouping("team")
            builder.setBolt("save-team", SaveTeamBolt(), 100).shuffleGrouping("team")


            val conf = Config()

            conf.setDebug(true)

            val cluster = LocalCluster()
            Thread.sleep(2000)

            cluster.submitTopology("topologija", conf, builder.createTopology())

            Thread.sleep(540000)

            cluster.killTopology("topologija")
        }
    }

}