package io.ventura.nexmark.cm;

import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8File;
import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.BidEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.beans.Query8WindowOutput;
import io.ventura.nexmark.beans.Serializer;
import io.ventura.nexmark.common.JoinHelper;
import io.ventura.nexmark.source.NexmarkAuctionSource;
import io.ventura.nexmark.source.NexmarkPersonSource;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.UUID;

import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.readProperty;
import static io.ventura.nexmark.source.AuctionsDeserializationSchema.AUCTION_RECORD_SIZE;
import static io.ventura.nexmark.source.BidDesearializationSchema.BID_RECORD_SIZE;
import static io.ventura.nexmark.source.PersonDeserializationSchema.PERSON_RECORD_SIZE;

public class CM {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8File.class);

	private static final int BUFFER_SIZE = 64 * 1024;

	static class CmOutput {
		double cpu = 0;
		long jobId = -1;

		CmOutput add(CmRecord r) {
			jobId = r.jobId;
			cpu += r.cpu;
			return this;
		}

		CmOutput merge(CmOutput other) {
			cpu += other.cpu;
			return this;
		}
	}

	public static void runCM(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int windowParallelism = params.getInt("windowParallelism", 1);
		final int windowDuration = params.getInt("windowDuration", 1);
		final int windowType = params.getInt("windowType", 0);
		Preconditions.checkArgument(windowDuration > 0);
		final int windowSlide = params.getInt("windowSlide", windowType == 1 ? windowDuration / 2 : windowDuration);
		final int sinkParallelism = params.getInt("sinkParallelism", windowParallelism);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);

		final boolean autogen = params.has("autogen");
		final String inputPathCM = params.getRequired("inputPath");

		final int numOfReplicaSlotsHint = params.getInt("numOfReplicaSlotsHint", 1);

		final String kafkaServers = params.get("kafkaServers", "localhost:9092");

		Properties baseCfg = new Properties();

		baseCfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		baseCfg.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + (4 * 1024 * 1024));
		baseCfg.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "32768");
		baseCfg.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "im-job");
		baseCfg.setProperty("offsets.commit.timeout.ms", "" + (3 * 60 * 1000));
		baseCfg.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "" + (10 * 1024 * 1024));
		baseCfg.setProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "false");

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.fallBackRestart());
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
			env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
			env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
		}
		env.setParallelism(parallelism);
		env.setMaxParallelism(maxParallelism);
		env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

		env.getConfig().enableForceKryo();
		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(CmRecord.class, Serializer.CmSerializer.class);
		env.getConfig().addDefaultKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(CmRecord.class, Serializer.CmSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().registerKryoType(BidEvent0.class);
		env.getConfig().registerKryoType(AuctionEvent0.class);
		env.getConfig().registerKryoType(NewPersonEvent0.class);
		env.getConfig().registerKryoType(CmRecord.class);
		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<CmRecord> in1;


		if (autogen) {

//			final long personToGenerate = params.getLong("personToGenerate", 1_000_000);
//			final long auctionsToGenerate = params.getLong("auctionsToGenerate", 10_000_000);
//			final int personRate = params.getInt("personRate", 1024 * 1024);
//			final int auctionRate = params.getInt("auctionRate", 10 * 1024 * 1024);
//
//
//			in1 = env.addSource(new NexmarkPersonSource(personToGenerate, personRate)).name("NewPersonsInputStream").setParallelism(sourceParallelism)
//					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
//						@Override
//						public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
//							return newPersonEvent.timestamp;
//						}
//					}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {
//					}));
//
//			in2 = env.addSource(new NexmarkAuctionSource(auctionsToGenerate, auctionRate)).name("BidEventInputStream").setParallelism(sourceParallelism)
//					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(1)) {
//						@Override
//						public long extractTimestamp(AuctionEvent0 auctionEvent) {
//							return auctionEvent.timestamp;
//						}
//					}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<AuctionEvent0>() {
//					}));


			throw new NotImplementedException("!");

		} else {
			in1 = env
					.addSource(new RichParallelSourceFunction<CmRecord>() {

						private transient FileChannel fc;
						private transient ByteBuffer mappedData;
						private transient String[] splits;
						private volatile boolean isRunning;

						@Override
						public void open(Configuration parameters) throws Exception {
							super.open(parameters);
							Path inputPath = Paths.get(inputPathCM);
							double startMapping = System.nanoTime();
							fc = FileChannel.open(inputPath, StandardOpenOption.READ);
							mappedData = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
							String str = StandardCharsets.UTF_8.decode(mappedData).toString();
							splits = str.split("\n");
							double endMapping = System.nanoTime();
							double diff = (endMapping - startMapping) / 1000;
							LOG.info("It took " + diff + " us to mmap the file");
							isRunning = true;
						}

						@Override
						public void run(SourceContext<CmRecord> sourceContext) throws Exception {
							int chunkSize = splits.length / getRuntimeContext().getNumberOfParallelSubtasks();
							int startOffset = getRuntimeContext().getIndexOfThisSubtask() * chunkSize;
							int endOffset = startOffset + chunkSize;
							while (isRunning) {
								double startMapping = System.nanoTime();
								for (int i = startOffset; i < endOffset; ++i) {
									sourceContext.collect(new CmRecord(splits[i]));
								}
								double endMapping = System.nanoTime();
								double diff = (endMapping - startMapping) / 1000;
								double throughput = chunkSize * 1_000_000.0 / diff;
								LOG.info("It took " + diff + " us to scan => " + throughput);
								return;
							}
						}

						@Override
						public void cancel() {
							isRunning = false;
						}
					})
					.name("CMSource").setParallelism(sourceParallelism)
//				.flatMap(new PersonsFlatMapper()).setParallelism(sourceParallelism)
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CmRecord>(Time.seconds(2)) {
						@Override
						public long extractTimestamp(CmRecord e) {
							return e.timestamp;
						}
					}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<CmRecord>() {
					}))
			;


		}
//		WindowAssigner<Object, TimeWindow> assigner = null;
//		switch (windowType) {
//			case 0:
//				assigner = TumblingEventTimeWindows.of(Time.seconds(windowDuration));
//				break;
//			case 1:
//				assigner = SlidingEventTimeWindows.of(Time.seconds(windowDuration), Time.seconds(windowSlide));
//				break;
//			case 2:
//				assigner = EventTimeSessionWindows.withGap(Time.seconds(windowDuration));
//				break;
//			default:
//				throw new IllegalStateException();
//		}
//
//		in1
//			.coGroup(in2)
//				.where(NewPersonEvent0::getPersonId)
//				.equalTo(AuctionEvent0::getPersonId)
//				.window(assigner)
//				.with(new JoiningNewUsersWithAuctionsCoGroupFunction())
//				.name("WindowOperator(" + windowDuration + ")")
//				.setParallelism(windowParallelism)
//				.setVirtualNodesNum(numOfVirtualNodes)
//				.setReplicaSlotsHint(numOfReplicaSlotsHint)
//			.addSink(new NexmarkQuery8LatencyTrackingSink())
//				.name("Nexmark8Sink")
//				.setParallelism(sinkParallelism);

		in1
				.keyBy(new KeySelector<CmRecord, Long>() {
					@Override
					public Long getKey(CmRecord e) throws Exception {
						return e.jobId;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.seconds(2)))
				.aggregate(new AggregateFunction<CmRecord, CmOutput, CmOutput>() {
					@Override
					public CmOutput createAccumulator() {
						return new CmOutput();
					}

					@Override
					public CmOutput add(CmRecord cmRecord, CmOutput acc) {
						return acc.add(cmRecord);
					}

					@Override
					public CmOutput getResult(CmOutput cmOutput) {
						return cmOutput;
					}

					@Override
					public CmOutput merge(CmOutput lhs, CmOutput rhs) {
						return lhs.merge(rhs);
					}
				})
				.name("CM")
				.uid(new UUID(0, 5).toString())
				.setParallelism(windowParallelism)
				.addSink(new RichSinkFunction<CmOutput>() {
					@Override
					public void invoke(CmOutput value, Context context) throws Exception {
						super.invoke(value, context);
					}
				})
				.name("CMSink")
				.setParallelism(sinkParallelism)
				.uid(new UUID(0, 6).toString());
	}


}
