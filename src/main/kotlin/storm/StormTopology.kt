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
//            builder.setSpout("team", TeamSpout(), 20)
//            builder.setSpout("team", TeamSpout(), 50)
//            builder.setSpout("team", TeamSpout(), 100)
            builder.setSpout("team", TeamSpout(), 120)



//            builder.setBolt("save-team", SaveTeamBolt(), 10).shuffleGrouping("team")
//            builder.setBolt("save-team", SaveTeamBolt(), 150).shuffleGrouping("team")
//            builder.setBolt("save-team", SaveTeamBolt(), 300).shuffleGrouping("team")
//            builder.setBolt("save-team", SaveTeamBolt(), 500).shuffleGrouping("team")
            builder.setBolt("save-team", SaveTeamBolt(), 750).shuffleGrouping("team")

            val conf = Config()

            conf.setDebug(true)
//            conf.setMaxTaskParallelism(500)
            conf.setMessageTimeoutSecs(1000)
            conf.setNumAckers(50)


            conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 20000)
            val cluster = LocalCluster()

            Thread.sleep(2000)

            // create the topology and submit with config
//            StormSubmitter.submitTopology("topologija", conf, builder.createTopology())
            cluster.submitTopology("topologija", conf, builder.createTopology())

            Thread.sleep(540000)

            cluster.killTopology("topologija")
        }
    }

}