package io.ventura.nexmark.NexmarkQuery11;

import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8File;
import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.BidEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.beans.Query8WindowOutput;
import io.ventura.nexmark.common.JoinHelper;
import io.ventura.nexmark.source.NexmarkAuctionSource;
import io.ventura.nexmark.source.NexmarkPersonSource;
import org.apache.commons.lang3.NotImplementedException;
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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.readProperty;
import static io.ventura.nexmark.source.AuctionsDeserializationSchema.AUCTION_RECORD_SIZE;
import static io.ventura.nexmark.source.BidDesearializationSchema.BID_RECORD_SIZE;
import static io.ventura.nexmark.source.PersonDeserializationSchema.PERSON_RECORD_SIZE;

public class NexmarkQuery11File {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8File.class);

	private static final int BUFFER_SIZE = 64 * 1024;

	public static void runNexmarkQ11File(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

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
		env.getConfig().addDefaultKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().registerKryoType(BidEvent0.class);
		env.getConfig().registerKryoType(AuctionEvent0.class);
		env.getConfig().registerKryoType(NewPersonEvent0.class);
		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<NewPersonEvent0> in1;
		DataStream<BidEvent0> in2;


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
					.addSource(new RichParallelSourceFunction<NewPersonEvent0>() {

						private transient FileChannel fc;
						private transient ByteBuffer mappedData;
						private volatile boolean isRunning;

						@Override
						public void open(Configuration parameters) throws Exception {
							super.open(parameters);
							File inputDir = new File(readProperty("nexmark.input.dir", System.getProperty("java.io.tmpdir")));
							Path inputPath = Paths.get(inputDir + "/persons_" + getRuntimeContext().getIndexOfThisSubtask() + ".bin");
							Preconditions.checkArgument(inputDir.exists());
							double startMapping = System.nanoTime();
							fc = FileChannel.open(inputPath, StandardOpenOption.READ);
							mappedData = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
							double endMapping = System.nanoTime();
							double diff = (endMapping - startMapping) / 1000;
							LOG.info("It took " + diff + " us to mmap the file");
							isRunning = true;
						}

						@Override
						public void run(SourceContext<NewPersonEvent0> sourceContext) throws Exception {
							int consumedSoFarBytes = 0;
							while (isRunning && mappedData.remaining() > 0) {
								int checksum = mappedData.getInt();
								int itemsInThisBuffer = mappedData.getInt();
								long newBacklog = mappedData.getLong();

								Preconditions.checkArgument(((BUFFER_SIZE - 16) / PERSON_RECORD_SIZE) >= itemsInThisBuffer);

								Preconditions.checkArgument(checksum == 0x30011991);

								byte[] tmp = new byte[32];

								long ingestionTimestamp = System.currentTimeMillis();

								StringBuilder helper = new StringBuilder(32 + 32 + 32 + 2);

								consumedSoFarBytes = 16;
								for (int i = 0; i < itemsInThisBuffer; i++) {
									long id = mappedData.getLong();
									mappedData.get(tmp);
									String name = new String(tmp);
									mappedData.get(tmp);
									String surname = new String(tmp);
									mappedData.get(tmp);
									String email = helper
											.append(name)
											.append(".")
											.append(surname)
											.append("@")
											.append(new String(tmp))
											.toString();
									//name + "." + surname + "@" + new String(Arrays.copyOf(tmp, tmp.length));
									mappedData.get(tmp);
									String city = new String(tmp);
									mappedData.get(tmp);
									String country = new String(tmp);
									long creditCard0 = mappedData.getLong();
									long creditCard1 = mappedData.getLong();
									int a = mappedData.getInt();
									int b = mappedData.getInt();
									int c = mappedData.getInt();
									short maleOrFemale = mappedData.getShort();
									long timestamp = mappedData.getLong(); // 128
									//				Preconditions.checkArgument(timestamp > 0);
									helper.setLength(0);
									sourceContext.collect(new NewPersonEvent0(
											timestamp,
											id,
											helper.append(name).append(" ").append(surname).toString(),
											email,
											city,
											country,
											"" + (a - c),
											"" + (b - c),
											email,
											"" + (creditCard0 + creditCard1),
											ingestionTimestamp));
									helper.setLength(0);
									consumedSoFarBytes += PERSON_RECORD_SIZE;
								}
								int remainingToConsume = BUFFER_SIZE - consumedSoFarBytes;
								if (remainingToConsume > 0) {
									mappedData.position(Math.min(mappedData.limit(), mappedData.position() + remainingToConsume));
								}
							}
						}

						@Override
						public void cancel() {
							isRunning = false;
						}
					})
					.name("NewPersonsInputStream").setParallelism(sourceParallelism)
//				.flatMap(new PersonsFlatMapper()).setParallelism(sourceParallelism)
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(2)) {
						@Override
						public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
							return newPersonEvent.timestamp;
						}
					}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {
					}))
			;

			in2 = env
					.addSource(new RichParallelSourceFunction<BidEvent0>() {

						private transient FileChannel fc;
						private transient ByteBuffer mappedData;
						private volatile boolean isRunning;
						private transient Path inputPath;

						@Override
						public void open(Configuration parameters) throws Exception {
							super.open(parameters);
							File inputDir = new File(readProperty("nexmark.input.dir", System.getProperty("java.io.tmpdir")));
							inputPath = Paths.get(inputDir + "/bids_" + getRuntimeContext().getIndexOfThisSubtask() + ".bin");
							Preconditions.checkArgument(inputDir.exists());
							double startMapping = System.nanoTime();
							fc = FileChannel.open(inputPath, StandardOpenOption.READ);
							mappedData = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
							double endMapping = System.nanoTime();
							double diff = (endMapping - startMapping) / 1000;
							LOG.info("It took " + diff + " us to mmap the file");
							isRunning = true;
						}

						@Override
						public void run(SourceContext<BidEvent0> sourceContext) throws Exception {
							int consumedSoFarBytes = 0;
							while (isRunning && mappedData.remaining() > 0) {
								int checksum = mappedData.getInt();
								int itemsInThisBuffer = mappedData.getInt();
								long newBacklog = mappedData.getLong();

								Preconditions.checkArgument(checksum == 0xdeedbeaf);
								Preconditions.checkArgument(((BUFFER_SIZE - 16) / BID_RECORD_SIZE) >= itemsInThisBuffer);

								long ingestionTimestamp = System.currentTimeMillis();
								consumedSoFarBytes = 16;
								for (int i = 0; i < itemsInThisBuffer; i++) {
									consumedSoFarBytes += BID_RECORD_SIZE;
									long bidderId = mappedData.getLong();
									long auctionId = mappedData.getLong();
									double price = mappedData.getDouble();
									long timestamp = mappedData.getLong();

									sourceContext
											.collect(
													BidEvent0.BIDS_RECYCLER.get().init(
															ingestionTimestamp, timestamp, auctionId, bidderId, -1, price
													)
											);
								}
								int remainingToConsume = BUFFER_SIZE - consumedSoFarBytes;
								if (remainingToConsume > 0) {
									mappedData.position(Math.min(mappedData.limit(), mappedData.position() + remainingToConsume));
								}
							}
						}

						@Override
						public void cancel() {
							isRunning = false;
						}
					})
					.name("BidEventInputStream").setParallelism(sourceParallelism)
//				.flatMap(new AuctionsFlatMapper()).setParallelism(sourceParallelism)
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<BidEvent0>(Time.seconds(2)) {
						@Override
						public long extractTimestamp(BidEvent0 auctionEvent) {
							return auctionEvent.timestamp;
						}
					}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<BidEvent0>() {
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

		JoinHelper.UnionTypeInfo<NewPersonEvent0, BidEvent0> unionType = new JoinHelper.UnionTypeInfo<>(in1.getType(), in2.getType());

		DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0>> taggedInput1 = in1
				.map(new JoinHelper.Input1Tagger<NewPersonEvent0, BidEvent0>())
				.setParallelism(in1.getParallelism())
				.returns(unionType);
		DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0>> taggedInput2 = in2
				.map(new JoinHelper.Input2Tagger<NewPersonEvent0, BidEvent0>())
				.setParallelism(in2.getParallelism())
				.returns(unionType);

		DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0>> unionStream = taggedInput1.union(taggedInput2);

		NexmarkQuery11File.JoinUDF function = new NexmarkQuery11File.JoinUDF();

		unionStream
				.keyBy(new KeySelector<JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0>, Long>() {
					@Override
					public Long getKey(JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0> value) throws Exception {
						return value.isOne() ? value.getOne().personId : value.getTwo().personId;
					}
				})
				.flatMap(function)
				.name("WindowOperator(" + windowDuration + ")")
				.setParallelism(windowParallelism)
//			.setVirtualNodesNum(numOfVirtualNodes)
//			.setReplicaSlotsHint(numOfReplicaSlotsHint)
				.addSink(new NexmarkQuery8.NexmarkQuery8LatencyTrackingSink("join_q11"))
				.name("Nexmark11Sink")
				.setParallelism(sinkParallelism);
	}


	public static final class JoinUDF
			extends RichFlatMapFunction<JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0>, Query8WindowOutput>
			implements CheckpointedFunction {


		private transient ValueState<NewPersonEvent0> activeUser;
		private transient ListState<BidEvent0> matchingBids;

		private long seenAuctions;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			seenAuctions = 0L;
		}

		@Override
		public void flatMap(
				JoinHelper.TaggedUnion<NewPersonEvent0, BidEvent0> in,
				Collector<Query8WindowOutput> out) throws Exception {
			if (in.isOne()) {
				NewPersonEvent0 p = in.getOne();
				activeUser.update(p);
				out.collect(new Query8WindowOutput(
						0L,
						p.timestamp,
						p.ingestionTimestamp,
						0L,
						0L,
						p.personId));
			} else {
				BidEvent0 b = in.getTwo();
				matchingBids.add(b);
				if (++seenAuctions % 200_000 == 0) {
					out.collect(new Query8WindowOutput(
							0L,
							0L,
							0L,
							b.timestamp,
							b.ingestionTimestamp,
							-b.personId));
				}
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ValueStateDescriptor<NewPersonEvent0> personDescriptor =
					new ValueStateDescriptor<NewPersonEvent0>("active-person", TypeInformation.of(NewPersonEvent0.class));

			ListStateDescriptor<BidEvent0> windowContentDescriptor =
				new ListStateDescriptor<>("window-contents", TypeInformation.of(BidEvent0.class));

			activeUser = context.getKeyedStateStore().getState(personDescriptor);
			matchingBids = context.getKeyedStateStore().getListState(windowContentDescriptor);
		}
	}

}
