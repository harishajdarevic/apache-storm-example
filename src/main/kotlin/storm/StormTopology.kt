package storm

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.topology.TopologyBuilder
import storm.bolt.SaveTeamBolt
import storm.spout.TeamSpout

class StormTopology {

    companion object {
        private val topologyName = "virginpulse-topology"
        private val spoutId = "team"
        private val boltId = "save-team"

        @JvmStatic
        fun main(args: Array<String>) {
            val builder = TopologyBuilder()

            builder.setSpout(spoutId, TeamSpout(), 10)
            builder.setBolt(boltId, SaveTeamBolt(), 10).shuffleGrouping(spoutId)

            val conf = Config()

            conf.setDebug(true)
            conf.setNumAckers(3)
            conf.setNumWorkers(3)
            conf[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 120
            conf[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 500

            Thread.sleep(2000)

            if(args.isNotEmpty() && args[0] == "local") {
                val cluster = LocalCluster()
                cluster.submitTopology(topologyName, conf, builder.createTopology())
            } else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology())
            }
        }
    }

}