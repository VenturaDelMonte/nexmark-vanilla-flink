package io.ventura.nexmark.NexmarkQuery5;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.ventura.nexmark.beans.BidEvent0;
import io.ventura.nexmark.common.BidsFlatMapper;
import io.ventura.nexmark.source.BidDesearializationSchema;
import io.ventura.nexmark.source.NexmarkBidSource;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AtomicDouble;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.readProperty;
import static io.ventura.nexmark.common.NexmarkCommon.BIDS_TOPIC;

public class NexmarkQuery5 {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery5.class);


	public static void runNexmarkQ5(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int windowParallelism = params.getInt("windowParallelism", 1);
		final int windowDuration = params.getInt("windowDuration", 2);
		final int windowSlide = params.getInt("windowSlide", 1);
		final int sinkParallelism = params.getInt("sinkParallelism", windowParallelism);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);
		final int sinkStride = params.getInt("sinkStride", NexmarkQuery4LatencyTrackingSink.DEFAULT_STRIDE);

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
		env.getConfig().setAutoWatermarkInterval(250);
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
		env.getConfig().registerTypeWithKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().registerKryoType(BidEvent0.class);
		env.getConfig().registerTypeWithKryoSerializer(NexmarkQuery4Accumulator.class, NexmarkQuery4AccumulatorSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NexmarkQuery4Accumulator.class, NexmarkQuery4AccumulatorSerializer.class);
		env.getConfig().registerKryoType(NexmarkQuery4Accumulator.class);
		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<BidEvent0> in;


		if (autogen) {

			final long bidToGenerate = params.getLong("bidToGenerate", 1_000_000);
			final int bidRate = params.getInt("bidRate", 1024 * 1024);


			in = env
					.addSource(new NexmarkBidSource(bidToGenerate, bidRate))
					.name("BidStream")
					.setParallelism(sourceParallelism)
					.uid(new UUID(0, 0).toString())
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<BidEvent0>(Time.seconds(1)) {
						@Override
						public long extractTimestamp(BidEvent0 e) {
						return e.timestamp;
					}
					})
					.setParallelism(sourceParallelism)
					.uid(new UUID(0, 1).toString())
					.returns(TypeInformation.of(new TypeHint<BidEvent0>() {}));




		} else {

			FlinkKafkaConsumer011<BidEvent0[]> kafkaBids =
				new FlinkKafkaConsumer011<>(BIDS_TOPIC, new BidDesearializationSchema(), baseCfg);

			kafkaBids.setCommitOffsetsOnCheckpoints(true);
			kafkaBids.setStartFromEarliest();

			in = env
					.addSource(kafkaBids)
					.name("BidsStream")
					.setParallelism(sourceParallelism)
					.uid(new UUID(0, 2).toString())
					.flatMap(new BidsFlatMapper())
					.setParallelism(sourceParallelism)
					.uid(new UUID(0, 3).toString())
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<BidEvent0>(Time.seconds(2)) {
						@Override
						public long extractTimestamp(BidEvent0 e) {
							return e.timestamp;
						}
					})
					.setParallelism(sourceParallelism)
					.uid(new UUID(0, 4).toString())
					.returns(TypeInformation.of(new TypeHint<BidEvent0>() {}))
			;

		}


		in
				.keyBy(new KeySelector<BidEvent0, Long>() {
					@Override
					public Long getKey(BidEvent0 e) throws Exception {
						return e.auctionId;
					}
				})
//				.window(TumblingEventTimeWindows.of(Time.seconds(windowDuration)))
				.window(SlidingEventTimeWindows.of(Time.seconds(windowDuration), Time.seconds(windowSlide)))
				.aggregate(new NexmarkQuery4Aggregator())
					.name("Nexmark4Aggregator")
					.uid(new UUID(0, 5).toString())
					.setParallelism(windowParallelism)
				.addSink(new NexmarkQuery4LatencyTrackingSink(sinkStride))
					.name("Nexmark4Sink")
					.setParallelism(sinkParallelism)
					.uid(new UUID(0, 6).toString());

	}

	private static final class NexmarkQuery4LatencyTrackingSink extends RichSinkFunction<NexmarkQuery4Output> implements Gauge<Double> {

		public static final int DEFAULT_STRIDE = 1;

		private static final long LATENCY_THRESHOLD = 10L * 60L * 1000L;

		private transient SummaryStatistics sinkLatencyBid;
		private transient SummaryStatistics sinkLatencyWindow;
		private transient SummaryStatistics sinkLatencyFlightTime;

		private transient BufferedWriter writer;

		private transient StringBuffer stringBuffer;

		private transient int index;

		private transient Thread cleaningHelper;

		private transient boolean logInit = false;

		private transient int writtenSoFar = 0;

		private transient long seenSoFar = 0;

		private final int stride;

		private transient AtomicInteger latency;

		public NexmarkQuery4LatencyTrackingSink(int stride) {
			this.stride = stride;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.sinkLatencyWindow = new SummaryStatistics();
			this.sinkLatencyBid = new SummaryStatistics();
			this.sinkLatencyFlightTime = new SummaryStatistics();
			this.stringBuffer = new StringBuffer(2048);
			this.index = getRuntimeContext().getIndexOfThisSubtask();

			File logDir = new File(readProperty("flink.sink.csv.dir", System.getProperty("java.io.tmpdir")));

			File logFile = new File(logDir, "latency_q5_" + index + ".csv");

			if (logFile.exists()) {
				this.writer = new BufferedWriter(new FileWriter(logFile, true));
				this.writer.write("\n");
			} else {
				this.writer = new BufferedWriter(new FileWriter(logFile, false));
				stringBuffer.append("subtask,ts,bidLatencyCount,flightTimeCount,bidLatencyMean,flightTimeMean,sinkLatencyWindow,bidLatencyStd,flightTimeStd,bidLatencyMin,flightTimeMin,bidLatencyMax,flightTimeMax");
				stringBuffer.append("\n");
				writer.write(stringBuffer.toString());
				writtenSoFar += stringBuffer.length() * 2;
			}

			cleaningHelper = ShutdownHookUtil.addShutdownHook(writer, getRuntimeContext().getTaskNameWithSubtasks(), LOG);

			stringBuffer.setLength(0);
			logInit = true;
			seenSoFar = 0;

			latency = new AtomicInteger(0);

			getRuntimeContext().getMetricGroup().gauge("bidsLatency", this);
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (logInit) {
				updateCSV(System.currentTimeMillis());
				writer.flush();
				writer.close();
			}

			sinkLatencyFlightTime.clear();
			sinkLatencyBid.clear();
		}

		private void updateCSV(long timestamp) throws IOException {
			try {
				stringBuffer.append(index);
				stringBuffer.append(",");
				stringBuffer.append(timestamp);
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyBid.getSum());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getSum());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyBid.getMean());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getMean());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyWindow.getMean());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyBid.getStandardDeviation());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getStandardDeviation());
				stringBuffer.append(",");


				stringBuffer.append(sinkLatencyBid.getMin());
				stringBuffer.append(",");
				stringBuffer.append(sinkLatencyFlightTime.getMin());
				stringBuffer.append(",");

				stringBuffer.append(sinkLatencyBid.getMax());
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
		public void invoke(NexmarkQuery4Output record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			long latency = timeMillis - record.lastTimestamp;
			if (latency <= LATENCY_THRESHOLD) {
				sinkLatencyBid.addValue(latency);
				sinkLatencyFlightTime.addValue(timeMillis - record.lastIngestionTimestamp);
				sinkLatencyWindow.addValue(timeMillis - record.windowTriggeringTimestamp);
				this.latency.lazySet((int) sinkLatencyBid.getMean());
				if (seenSoFar++ % stride == 0) {
					updateCSV(timeMillis);
				}
			}
		}

		@Override
		public Double getValue() {
			return (double) latency.get();
		}
	}

	private static class NexmarkQuery4Aggregator implements AggregateFunction<BidEvent0, NexmarkQuery4Accumulator, NexmarkQuery4Output> {
		@Override
		public NexmarkQuery4Accumulator createAccumulator() {
			return new NexmarkQuery4Accumulator(-1, Double.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
		}

		@Override
		public NexmarkQuery4Accumulator add(BidEvent0 e, NexmarkQuery4Accumulator acc) {
			return acc.add(e);
		}

		@Override
		public NexmarkQuery4Output getResult(NexmarkQuery4Accumulator acc) {
			return acc.toOutput();
		}

		@Override
		public NexmarkQuery4Accumulator merge(NexmarkQuery4Accumulator left, NexmarkQuery4Accumulator right) {
			return left.merge(right);
		}
	}

	public static class NexmarkQuery4AccumulatorSerializer extends com.esotericsoftware.kryo.Serializer<NexmarkQuery4Accumulator> {

		@Override
		public void write(Kryo kryo, Output output, NexmarkQuery4Accumulator a) {
			output.writeLong(a.auction);
			output.writeDouble(a.maxPrice);
			output.writeLong(a.lastTimestamp);
			output.writeLong(a.lastIngestionTimestamp);
		}

		@Override
		public NexmarkQuery4Accumulator read(Kryo kryo, Input input, Class<NexmarkQuery4Accumulator> clz) {
			long id = input.readLong();
			double maxPrice = input.readDouble();
			long ts = input.readLong();
			long lastTs = input.readLong();
			return new NexmarkQuery4Accumulator(id, maxPrice, ts, lastTs);
		}
	}

	public static class NexmarkQuery4Accumulator implements Serializable {

		public long auction = -1;
		public long lastIngestionTimestamp = 0;
		public long lastTimestamp = 0;
		public double maxPrice = 0;

		public NexmarkQuery4Accumulator(long id, double maxPrice, long ts, long lastTs) {
			this.auction = id;
			this.maxPrice = maxPrice;
			this.lastTimestamp = ts;
			this.lastIngestionTimestamp = lastTs;
		}

		public NexmarkQuery4Accumulator add(BidEvent0 e) {
			maxPrice = Math.max(maxPrice, e.bid);
			auction = e.auctionId;
//			lastIngestionTimestamp = e.ingestionTimestamp;
//			lastTimestamp = e.timestamp;
			if (lastTimestamp < e.timestamp) {
				lastTimestamp = e.timestamp;
				lastIngestionTimestamp = e.ingestionTimestamp;
			}
			return this;
		}

		public NexmarkQuery4Output toOutput() {
			return new NexmarkQuery4Output(lastTimestamp, lastIngestionTimestamp);
		}

		public NexmarkQuery4Accumulator merge(NexmarkQuery4Accumulator that) {
			maxPrice = Math.max(maxPrice, that.maxPrice);
			lastTimestamp = Math.max(lastTimestamp, that.lastTimestamp);
			lastIngestionTimestamp = Math.max(lastIngestionTimestamp, that.lastIngestionTimestamp);
			return this;
		}
	}

	private static class NexmarkQuery4Output implements Serializable {

		public long lastIngestionTimestamp = 0;
		public long lastTimestamp = 0;
		public long windowTriggeringTimestamp = 0;

		public NexmarkQuery4Output(long timestamp, long ingestionTimestamp) {
			this.lastIngestionTimestamp = timestamp;
			this.lastTimestamp = ingestionTimestamp;
			this.windowTriggeringTimestamp = System.currentTimeMillis();
		}
	}
}
