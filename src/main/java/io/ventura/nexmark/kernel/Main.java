package io.ventura.nexmark.kernel;

import io.ventura.nexmark.NexmarkQuery11.NexmarkQuery11File;
import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5File;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8File;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5.runNexmarkQ5;
import static io.ventura.nexmark.NexmarkQuery5b.NexmarkQuery5b.runNexmarkQ5b;
import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.runNexmarkQ8;
import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.runNexmarkQ8Debug;
import static io.ventura.nexmark.NexmarkQueryX.NexmarkQueryX.runNexmarkQX;
import static io.ventura.nexmark.cm.CM.runCM;
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
			} else if (params.has("cm")) {
				runCM(env, params);
				query = "CM";
			} else if (params.has("q8fc")) {
				NexmarkQuery8File.runNexmarkQ8File(env, params);
				query = "Query 8 File";
			} else if (params.has("q5fc")) {
				NexmarkQuery5File.runNexmarkQ5(env, params);
				query = "Query 5 File";
			} else if (params.has("q11fc")) {
				NexmarkQuery11File.runNexmarkQ11File(env, params);
				query = "Query 11 File";
			} else if (params.has("q5")) {
				query = "Query 5";
				runNexmarkQ5(env, params);
			} else if (params.has("qx")) {
				query = "Query X";
				runNexmarkQX(env, params);
			} else if (params.has("q0")) {
				query = "Generator";
				runGenerator(env, params);
			} else if (params.has("q5b")) {
				query = "Query 5b";
				runNexmarkQ5b(env, params);
			} else {
				throw new UnsupportedOperationException();
			}
			env.execute("Nexmark " + query);
		} catch (Exception error) {
			LOG.error("Error", error);
		}


	}

}
