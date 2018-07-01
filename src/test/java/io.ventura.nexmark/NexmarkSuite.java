package io.ventura.nexmark;

import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NexmarkSuite {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkSuite.class);


	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 4;


	private static TestingCluster cluster;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		// detect parameter change

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
		config.setInteger(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), 2048);

		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setInteger(WebOptions.PORT, 8081);

		config.setString(CheckpointingOptions.STATE_BACKEND, "rocksdb");
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink");

		cluster = new TestingCluster(config);
		cluster.start();

	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.stop();
		}
	}

	@Test
	public void runNexmarkDebug() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("personsInputSizeGb", "1");
		config.put("desiredPersonsThroughputMb", "100");
		//config.put("checkpointingInterval", "5000");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmarkDebug(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkFull() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("personsInputSizeGb", "1");
		config.put("checkpointingInterval", "5000");
		config.put("windowParallelism", "4");
		config.put("sourceParallelism", "2");
		config.put("sinkParallelism", "4");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmark(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}
}
