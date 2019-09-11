package io.ventura.nexmark;

import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import io.ventura.nexmark.NexmarkQueryX.NexmarkQueryX;
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


	private static final int numTaskManagers = 4;
	private static final int slotsPerTaskManager = 1;


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
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + System.getProperty("java.io.tmpdir"));
		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, false);




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

		config.put("autogen", "1");
		config.put("personsInputSizeGb", "1");
		config.put("desiredPersonsThroughputMb", "100");
		//config.put("checkpointingInterval", "5000");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmarkQ8Debug(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQ8() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("windowDuration", "500000");
		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmarkQ8(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQ5() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery5.runNexmarkQ5(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQ5Kafka() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery5.runNexmarkQ5(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQX() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQueryX.runNexmarkQX(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}
}
