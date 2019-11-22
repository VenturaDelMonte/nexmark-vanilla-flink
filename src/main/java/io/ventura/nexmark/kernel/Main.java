package io.ventura.nexmark.kernel;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5.runNexmarkQ5;
import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.runNexmarkQ8;
import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.runNexmarkQ8Debug;
import static io.ventura.nexmark.NexmarkQueryX.NexmarkQueryX.runNexmarkQX;
import static io.ventura.nexmark.generator.GeneratorPipeline.runGenerator;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterTool.fromArgs(args);

		String query = "";
		try {
			if (params.has("debug")) {
				runNexmarkQ8Debug(env, params);
				query = "Query 8 Debug";
			} else if (params.has("q8")) {
				runNexmarkQ8(env, params);
				query = "Query 8";
			} else if (params.has("q5")) {
				query = "Query 5";
				runNexmarkQ5(env, params);
			} else if (params.has("qx")) {
				query = "Query X";
				runNexmarkQX(env, params);
			} else if (params.has("qx")) {
				query = "Generator";
				runGenerator(env, params);
			} else {
				throw new UnsupportedOperationException();
			}
			env.execute("Nexmark " + query);
		} catch (Exception error) {
			LOG.error("Error", error);
		}


	}

}
