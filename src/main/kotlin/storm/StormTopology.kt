package storm

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import storm.bolt.SaveAddressBolt
import storm.spout.AddressSpout

class StormTopology {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val builder = TopologyBuilder()

            builder.setSpout("address", AddressSpout(), 1)

            builder.setBolt("save-address", SaveAddressBolt(), 1).shuffleGrouping("address")

            val conf = Config()
//            conf.setNumWorkers(3)
//            conf.setMaxTaskParallelism(3)
            conf.setDebug(true)

            val cluster = LocalCluster()
            Thread.sleep(10000)

            cluster.submitTopology("topologija", conf, builder.createTopology())

            Thread.sleep(10000)

            cluster.killTopology("topology")
        }
    }

}