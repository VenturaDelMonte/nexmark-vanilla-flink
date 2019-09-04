package io.ventura.nexmark.NexmarkQuery8;

import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.beans.Query8WindowOutput;
import io.ventura.nexmark.source.AuctionsDeserializationSchema;
import io.ventura.nexmark.source.NexmarkAuctionSource;
import io.ventura.nexmark.source.NexmarkPersonSource;
import io.ventura.nexmark.source.PersonDeserializationSchema;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static io.ventura.nexmark.common.NexmarkCommon.AUCTIONS_TOPIC;
import static io.ventura.nexmark.common.NexmarkCommon.PERSONS_TOPIC;

public class NexmarkQuery8 {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8.class);

	private static final long ONE_GIGABYTE = 1024L * 1024L * 1024L;

	public static class JoiningNewUsersWithAuctionsCoGroupFunction extends RichCoGroupFunction<NewPersonEvent0, AuctionEvent0, Query8WindowOutput> {

		private static final Logger LOG = LoggerFactory.getLogger(JoiningNewUsersWithAuctionsCoGroupFunction.class);

		/**
		 * CoGroups Auction and Person on person id and return the Persons name as well as ID.
		 * Finding every person that created a new auction.
		 *
		 * Currently, when execution on the simple generator, it most certainly will happen, that the same person
		 * appears multiple times in a window. Currently, simple ignore that case.
		 */
		@Override
		public void coGroup(
				Iterable<NewPersonEvent0> persons,
				Iterable<AuctionEvent0> auctions,
				Collector<Query8WindowOutput> out) {

			Iterator<NewPersonEvent0> personIterator = persons.iterator();
			Iterator<AuctionEvent0> auctionIterator = auctions.iterator();

			if (!auctionIterator.hasNext()) {
				return;
			}

			while (personIterator.hasNext()) {
				NewPersonEvent0 person = personIterator.next();

				long ts = System.currentTimeMillis();
				long auctionCreationTimestampLatest = Long.MIN_VALUE;
				long auctionIngestionTimestampLatest = Long.MIN_VALUE;
				for (AuctionEvent0 auction : auctions) {
					long auctionIngestionTimestamp = auction.getIngestionTimestamp();
					if (auctionIngestionTimestamp > auctionIngestionTimestampLatest) {
						auctionIngestionTimestampLatest = auctionIngestionTimestamp;
						auctionCreationTimestampLatest = auction.getTimestamp();
					}
				}

				out.collect(new Query8WindowOutput(
							ts,
							person.getTimestamp(),
							person.getIngestionTimestamp(),
							auctionCreationTimestampLatest,
							auctionIngestionTimestampLatest,
							person.getPersonId()));
			}
		}
	}

	private static final class PersonsFlatMapper implements FlatMapFunction<NewPersonEvent0[], NewPersonEvent0> {
		@Override
		public void flatMap(NewPersonEvent0[] items, Collector<NewPersonEvent0> out) throws Exception {
			for (int i = 0; i < items.length; i++) {
				out.collect(items[i]);
			}
		}
	}

	private static final class AuctionsFlatMapper implements FlatMapFunction<AuctionEvent0[], AuctionEvent0> {
		@Override
		public void flatMap(AuctionEvent0[] items, Collector<AuctionEvent0> out) throws Exception {
			for (int i = 0; i < items.length; i++) {
				out.collect(items[i]);
			}
		}
	}

	private static final class SinkLatencyTrackingHistogramStatistics extends HistogramStatistics {

		private final SummaryStatistics impl;

		public SinkLatencyTrackingHistogramStatistics(SummaryStatistics original) {
			this.impl = original.copy();
		}

		@Override
		public double getQuantile(double v) {
			return -1;
		}

		@Override
		public long[] getValues() {
			return new long[] {-1};
		}

		@Override
		public int size() {
			return (int) impl.getN();
		}

		@Override
		public double getMean() {
			return impl.getMean();
		}

		@Override
		public double getStdDev() {
			return impl.getStandardDeviation();
		}

		@Override
		public long getMax() {
			return (long) impl.getMax();
		}

		@Override
		public long getMin() {
			return (long) impl.getMin();
		}
	}

	private static final class SinkLatencyTrackingHistogram implements Histogram {

		private final SummaryStatistics impl = new SummaryStatistics();

		@Override
		public void update(long l) {
			impl.addValue(l);
		}

		@Override
		public long getCount() {
			return impl.getN();
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new SinkLatencyTrackingHistogramStatistics(impl);
		}
	}

	public static String readProperty(final String key, String def) {
		if (key == null) {
			throw new NullPointerException("key");
		} else if (key.isEmpty()) {
			throw new IllegalArgumentException("key must not be empty.");
		} else {
			String value = null;

			try {
				if (System.getSecurityManager() == null) {
					value = System.getProperty(key);
				} else {
					value = (String) AccessController.doPrivileged(new PrivilegedAction<String>() {
						public String run() {
							return System.getProperty(key);
						}
					});
				}
			} catch (SecurityException var4) {
				LOG.warn("Unable to retrieve a system property '{}'; default values will be used.", key, var4);
			}

			return value == null ? def : value;
		}
	}

	private static final class NexmarkQuery8LatencyTrackingSink extends RichSinkFunction<Query8WindowOutput> {

		private static final long LATENCY_THRESHOLD = 10L * 60L * 1000L;

		private transient SummaryStatistics sinkLatencyPersonCreation;
		private transient SummaryStatistics sinkLatencyAuctionCreation;
		private transient SummaryStatistics sinkLatencyFlightTime;

		private transient BufferedWriter writer;

		private transient StringBuffer stringBuffer;

		private transient int index;

		private transient Thread cleaningHelper;

		private transient boolean logInit = false;

		private transient int writtenSoFar = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
//			buffer = new StringBuilder(256);
//			sinkLatencyWindowEviction = getRuntimeContext().getMetricGroup().histogram("sinkLatencyWindowEviction", new SinkLatencyTrackingHistogram());
//			sinkLatencyPersonCreation = getRuntimeContext().getMetricGroup().histogram("sinkLatencyPersonCreation", new SinkLatencyTrackingHistogram());
//			sinkLatencyAuctionCreation = getRuntimeContext().getMetricGroup().histogram("sinkLatencyAuctionCreation", new SinkLatencyTrackingHistogram());
//			sinkLatencyFlightTime = getRuntimeContext().getMetricGroup().histogram("sinkLatencyFlightTime", new SinkLatencyTrackingHistogram());

			this.sinkLatencyPersonCreation = new SummaryStatistics();
			this.sinkLatencyAuctionCreation = new SummaryStatistics();
			this.sinkLatencyFlightTime = new SummaryStatistics();
			this.stringBuffer = new StringBuffer(2048);
			this.index = getRuntimeContext().getIndexOfThisSubtask();

			File logDir = new File(readProperty("flink.sink.csv.dir", System.getProperty("java.io.tmpdir")));

			File logFile = new File(logDir, "latency_" + index + ".csv");

			if (logFile.exists()) {
				this.writer = new BufferedWriter(new FileWriter(logFile, true));
				this.writer.write("\n");
			} else {
				this.writer = new BufferedWriter(new FileWriter(logFile, false));
				stringBuffer.append("subtask,ts,personCount,auctionCount,flightTimeCount,personMean,auctionMean,flightTimeMean,personStd,auctionStd,flightTimeStd,personMin,auctionMin,flightTimeMin,personMax,auctionMax,flightTimeMax");
				stringBuffer.append("\n");
				writer.write(stringBuffer.toString());
				writtenSoFar += stringBuffer.length() * 2;
			}

			cleaningHelper = ShutdownHookUtil.addShutdownHook(writer, getRuntimeContext().getTaskNameWithSubtasks(), LOG);

			stringBuffer.setLength(0);
			logInit = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (logInit) {
				updateCSV(System.currentTimeMillis());
				writer.flush();
				writer.close();
			}

			sinkLatencyPersonCreation.clear();
			sinkLatencyFlightTime.clear();
			sinkLatencyAuctionCreation.clear();
		}

		private void updateCSV(long timestamp) throws IOException {
			try {
				stringBuffer.append(index);
				stringBuffer.append(",");
				stringBuffer.append(timestamp);
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyPersonCreation.getSum());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyAuctionCreation.getSum());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getSum());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyPersonCreation.getMean());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyAuctionCreation.getMean());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getMean());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyPersonCreation.getStandardDeviation());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyAuctionCreation.getStandardDeviation());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getStandardDeviation());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyPersonCreation.getMin());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyAuctionCreation.getMin());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getMin());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyPersonCreation.getMax());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyAuctionCreation.getMax());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getMax());

				stringBuffer.append("\n");

				writer.write(stringBuffer.toString());

				writtenSoFar += stringBuffer.length() * 2;
				if (writtenSoFar >= (8 * 1024 * 1024)) {
					writer.flush();
					writtenSoFar = 0;
				}

			} finally {
				stringBuffer.setLength(0);
			}
		}

		@Override
		public void invoke(Query8WindowOutput record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			if ((record.getPersonId() > 0)) {
				long latency = timeMillis - record.getPersonCreationTimestamp();
				if (latency < LATENCY_THRESHOLD) {
					sinkLatencyPersonCreation.addValue(latency);
				}
			} else {
				long latency = timeMillis - record.getAuctionCreationTimestamp();
				if (latency <= LATENCY_THRESHOLD) {
					sinkLatencyAuctionCreation.addValue(latency);
					sinkLatencyFlightTime.addValue(timeMillis - record.getAuctionIngestionTimestamp());
					updateCSV(timeMillis);
				}
			}
//			sinkLatencyPersonCreation.update(timeMillis - record.getPersonCreationTimestamp());
//			sinkLatencyWindowEviction.update(timeMillis - record.getWindowEvictingTimestamp());
//			sinkLatencyAuctionCreation.update(timeMillis - record.getAuctionCreationTimestamp());
////			try {
//				buffer.append(timeMillis);
//				buffer.append(",");
//				buffer.append(timeMillis - record.getWindowEvictingTimestamp());
//				buffer.append(",");
//				buffer.append(timeMillis - record.getAuctionCreationTimestamp());
//				buffer.append(",");
//				buffer.append(timeMillis - record.getPersonCreationTimestamp());
//				buffer.append(",");
//				buffer.append(record.getPersonId());
//				LOG.info("Nexmark8Sink - {}", buffer.toString());
//			} finally {
//				buffer.setLength(0);
//			}
		}
	}

	public static void runNexmarkQ8(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

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
		env.getConfig().registerKryoType(AuctionEvent0.class);
		env.getConfig().registerKryoType(NewPersonEvent0.class);
		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<NewPersonEvent0> in1;
		DataStream<AuctionEvent0> in2;


		if (autogen) {

			final long personToGenerate = params.getLong("personToGenerate", 1_000_000);
			final long auctionsToGenerate = params.getLong("auctionsToGenerate", 10_000_000);
			final int personRate = params.getInt("personRate", 1024 * 1024);
			final int auctionRate = params.getInt("auctionRate", 10 * 1024 * 1024);


			in1 = env.addSource(new NexmarkPersonSource(personToGenerate, personRate)).name("NewPersonsInputStream").setParallelism(sourceParallelism)
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
					@Override
					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
						return newPersonEvent.timestamp;
					}
			}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {}));

			in2 = env.addSource(new NexmarkAuctionSource(auctionsToGenerate, auctionRate)).name("AuctionEventInputStream").setParallelism(sourceParallelism)
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(1)) {
				@Override
				public long extractTimestamp(AuctionEvent0 auctionEvent) {
					return auctionEvent.timestamp;
				}
			}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<AuctionEvent0>() {}));


		} else {

			FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSourcePersons =
				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

			FlinkKafkaConsumer011<AuctionEvent0[]> kafkaSourceAuctions =
					new FlinkKafkaConsumer011<>(AUCTIONS_TOPIC, new AuctionsDeserializationSchema(), baseCfg);

			kafkaSourceAuctions.setCommitOffsetsOnCheckpoints(true);
			kafkaSourceAuctions.setStartFromEarliest();
			kafkaSourcePersons.setCommitOffsetsOnCheckpoints(true);
			kafkaSourcePersons.setStartFromEarliest();

			in1 = env
				.addSource(kafkaSourcePersons)
				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
				.flatMap(new PersonsFlatMapper()).setParallelism(sourceParallelism)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(2)) {
					@Override
					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
						return newPersonEvent.timestamp;
					}
				}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {}))
			;

			in2 = env
				.addSource(kafkaSourceAuctions)
				.name("AuctionEventInputStream").setParallelism(sourceParallelism)
				.flatMap(new AuctionsFlatMapper()).setParallelism(sourceParallelism)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(2)) {
					@Override
					public long extractTimestamp(AuctionEvent0 auctionEvent) {
						return auctionEvent.timestamp;
					}
				}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<AuctionEvent0>() {}))
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

		UnionTypeInfo<NewPersonEvent0, AuctionEvent0> unionType = new UnionTypeInfo<>(in1.getType(), in2.getType());

		DataStream<TaggedUnion<NewPersonEvent0, AuctionEvent0>> taggedInput1 = in1
				.map(new Input1Tagger<NewPersonEvent0, AuctionEvent0>())
				.setParallelism(in1.getParallelism())
				.returns(unionType);
		DataStream<TaggedUnion<NewPersonEvent0, AuctionEvent0>> taggedInput2 = in2
				.map(new Input2Tagger<NewPersonEvent0, AuctionEvent0>())
				.setParallelism(in2.getParallelism())
				.returns(unionType);

		DataStream<TaggedUnion<NewPersonEvent0, AuctionEvent0>> unionStream = taggedInput1.union(taggedInput2);

		JoinUDF function = new JoinUDF();

		unionStream
			.keyBy(new KeySelector<TaggedUnion<NewPersonEvent0, AuctionEvent0>, Long>() {
				@Override
				public Long getKey(TaggedUnion<NewPersonEvent0, AuctionEvent0> value) throws Exception {
					return value.isOne() ? value.getOne().personId : value.getTwo().personId;
				}
			})
			.flatMap(function)
			.name("WindowOperator(" + windowDuration + ")")
			.setParallelism(windowParallelism)
//			.setVirtualNodesNum(numOfVirtualNodes)
//			.setReplicaSlotsHint(numOfReplicaSlotsHint)
		.addSink(new NexmarkQuery8LatencyTrackingSink())
			.name("Nexmark8Sink")
			.setParallelism(sinkParallelism);
	}

	private static final class JoinUDF
			extends RichFlatMapFunction<TaggedUnion<NewPersonEvent0, AuctionEvent0>, Query8WindowOutput>
			implements CheckpointedFunction {


		private transient ValueState<NewPersonEvent0> activeUser;
		private transient ListState<AuctionEvent0> matchingAuctions;

		private long seenAuctions;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			seenAuctions = 0L;
		}

		@Override
		public void flatMap(
				TaggedUnion<NewPersonEvent0, AuctionEvent0> in,
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
				AuctionEvent0 a = in.getTwo();
				matchingAuctions.add(a);
				if (++seenAuctions % 200_000 == 0) {
					out.collect(new Query8WindowOutput(
							0L,
							0L,
							0L,
							a.timestamp,
							a.ingestionTimestamp,
							-a.personId));
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

			ListStateDescriptor<AuctionEvent0> windowContentDescriptor =
				new ListStateDescriptor<>("window-contents", TypeInformation.of(AuctionEvent0.class));

			activeUser = context.getKeyedStateStore().getState(personDescriptor);
			matchingAuctions = context.getKeyedStateStore().getListState(windowContentDescriptor);
		}
	}

	private static class Input1Tagger<T1, T2> implements MapFunction<T1, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T1 value) throws Exception {
			return TaggedUnion.one(value);
		}
	}

	private static class Input2Tagger<T1, T2> implements MapFunction<T2, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T2 value) throws Exception {
			return TaggedUnion.two(value);
		}
	}

	private static class UnionKeySelector<T1, T2, KEY> implements KeySelector<TaggedUnion<T1, T2>, KEY> {
		private static final long serialVersionUID = 1L;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;

		public UnionKeySelector(KeySelector<T1, KEY> keySelector1,
				KeySelector<T2, KEY> keySelector2) {
			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;
		}

		@Override
		public KEY getKey(TaggedUnion<T1, T2> value) throws Exception{
			if (value.isOne()) {
				return keySelector1.getKey(value.getOne());
			} else {
				return keySelector2.getKey(value.getTwo());
			}
		}
	}

	public static class TaggedUnion<T1, T2> {
		private final T1 one;
		private final T2 two;

		private TaggedUnion(T1 one, T2 two) {
			this.one = one;
			this.two = two;
		}

		public boolean isOne() {
			return one != null;
		}

		public boolean isTwo() {
			return two != null;
		}

		public T1 getOne() {
			return one;
		}

		public T2 getTwo() {
			return two;
		}

		public static <T1, T2> TaggedUnion<T1, T2> one(T1 one) {
			return new TaggedUnion<>(one, null);
		}

		public static <T1, T2> TaggedUnion<T1, T2> two(T2 two) {
			return new TaggedUnion<>(null, two);
		}
	}

	private static class UnionTypeInfo<T1, T2> extends TypeInformation<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeInformation<T1> oneType;
		private final TypeInformation<T2> twoType;

		public UnionTypeInfo(TypeInformation<T1> oneType,
				TypeInformation<T2> twoType) {
			this.oneType = oneType;
			this.twoType = twoType;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 2;
		}

		@Override
		public int getTotalFields() {
			return 2;
		}

		@Override
		@SuppressWarnings("unchecked, rawtypes")
		public Class<TaggedUnion<T1, T2>> getTypeClass() {
			return (Class) TaggedUnion.class;
		}

		@Override
		public boolean isKeyType() {
			return true;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> createSerializer(ExecutionConfig config) {
			return new UnionSerializer<>(oneType.createSerializer(config), twoType.createSerializer(config));
		}

		@Override
		public String toString() {
			return "TaggedUnion<" + oneType + ", " + twoType + ">";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UnionTypeInfo) {
				@SuppressWarnings("unchecked")
				UnionTypeInfo<T1, T2> unionTypeInfo = (UnionTypeInfo<T1, T2>) obj;

				return unionTypeInfo.canEqual(this) && oneType.equals(unionTypeInfo.oneType) && twoType.equals(unionTypeInfo.twoType);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 *  oneType.hashCode() + twoType.hashCode();
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionTypeInfo;
		}
	}

	private static class UnionSerializer<T1, T2> extends TypeSerializer<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeSerializer<T1> oneSerializer;
		private final TypeSerializer<T2> twoSerializer;

		public UnionSerializer(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			this.oneSerializer = oneSerializer;
			this.twoSerializer = twoSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> duplicate() {
			return this;
		}

		@Override
		public TaggedUnion<T1, T2> createInstance() {
			return null;
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from, TaggedUnion<T1, T2> reuse) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(TaggedUnion<T1, T2> record, DataOutputView target) throws IOException {
			if (record.isOne()) {
				target.writeByte(1);
				oneSerializer.serialize(record.getOne(), target);
			} else {
				target.writeByte(2);
				twoSerializer.serialize(record.getTwo(), target);
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(TaggedUnion<T1, T2> reuse,
				DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			byte tag = source.readByte();
			target.writeByte(tag);
			if (tag == 1) {
				oneSerializer.copy(source, target);
			} else {
				twoSerializer.copy(source, target);
			}
		}

		@Override
		public int hashCode() {
			return 31 * oneSerializer.hashCode() + twoSerializer.hashCode();
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object obj) {
			if (obj instanceof UnionSerializer) {
				UnionSerializer<T1, T2> other = (UnionSerializer<T1, T2>) obj;

				return other.canEqual(this) && oneSerializer.equals(other.oneSerializer) && twoSerializer.equals(other.twoSerializer);
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionSerializer;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new UnionSerializerConfigSnapshot<>(oneSerializer, twoSerializer);
		}

		@Override
		public CompatibilityResult<TaggedUnion<T1, T2>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof UnionSerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
					((UnionSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T1> oneSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(0).f1,
					oneSerializer);

				CompatibilityResult<T2> twoSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(1).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(1).f1,
					twoSerializer);

				if (!oneSerializerCompatResult.isRequiresMigration() && !twoSerializerCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else if (oneSerializerCompatResult.getConvertDeserializer() != null && twoSerializerCompatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
						new UnionSerializer<>(
							new TypeDeserializerAdapter<>(oneSerializerCompatResult.getConvertDeserializer()),
							new TypeDeserializerAdapter<>(twoSerializerCompatResult.getConvertDeserializer())));
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} for the {@link UnionSerializer}.
	 */
	public static class UnionSerializerConfigSnapshot<T1, T2> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public UnionSerializerConfigSnapshot() {}

		public UnionSerializerConfigSnapshot(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			super(oneSerializer, twoSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}


	public static void runNexmarkQ8Debug(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);
		final String kafkaServers = params.get("kafkaServers", "localhost:9092");
		final int personStreamSizeBytes = params.getInt("personStreamSizeBytes", 1);

		Properties baseCfg = new Properties();

		baseCfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		baseCfg.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + (128 * 1024));

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.noRestart());
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
			env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
		}
		env.getConfig().enableObjectReuse();
		env.setParallelism(parallelism);
		env.setMaxParallelism(maxParallelism);
		env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);

		FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSource =
				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

		kafkaSource.setStartFromEarliest();
		kafkaSource.setCommitOffsetsOnCheckpoints(true);

		env
				.addSource(kafkaSource)
				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
				.flatMap(new FlatMapFunction<NewPersonEvent0[], NewPersonEvent0>() {
					@Override
					public void flatMap(NewPersonEvent0[] items, Collector<NewPersonEvent0> out) throws Exception {
						for (NewPersonEvent0 item : items) {
							out.collect(item);
						}
					}
				}).setParallelism(sourceParallelism)
//				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
//					@Override
//					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
//						return newPersonEvent.getTimestamp();
//					}
//				})
				.addSink(new SinkFunction<NewPersonEvent0>() {
					@Override
					public void invoke(NewPersonEvent0 value, Context context) throws Exception {

					}
				}).setParallelism(sourceParallelism);

	}




}
