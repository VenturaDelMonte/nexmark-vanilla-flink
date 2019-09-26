package io.ventura.nexmark.NexmarkQueryX;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.BidEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.common.AuctionsFlatMapper;
import io.ventura.nexmark.common.BidsFlatMapper;
import io.ventura.nexmark.common.JoinHelper;
import io.ventura.nexmark.common.PersonsFlatMapper;
import io.ventura.nexmark.source.AuctionsDeserializationSchema;
import io.ventura.nexmark.source.BidDesearializationSchema;
import io.ventura.nexmark.source.NexmarkAuctionSource;
import io.ventura.nexmark.source.NexmarkBidSource;
import io.ventura.nexmark.source.NexmarkPersonSource;
import io.ventura.nexmark.source.PersonDeserializationSchema;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AtomicDouble;
import org.apache.flink.shaded.netty4.io.netty.util.Recycler;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8.readProperty;
import static io.ventura.nexmark.common.NexmarkCommon.AUCTIONS_TOPIC;
import static io.ventura.nexmark.common.NexmarkCommon.BIDS_TOPIC;
import static io.ventura.nexmark.common.NexmarkCommon.PERSONS_TOPIC;

public class NexmarkQueryX {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQueryX.class);


	public static void runNexmarkQX(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int windowParallelism = params.getInt("windowParallelism", 1);
		final int windowDuration = params.getInt("windowDuration", 60);
		final int windowSlide = params.getInt("windowSlide", 2);
		final int sinkParallelism = params.getInt("sinkParallelism", windowParallelism);

		final int sessionDuration = params.getInt("sessionDuration", 60);
		final int sessionAllowedLateness = params.getInt("sessionAllowedLateness", 15);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);
		final int timeout = params.getInt("timeout", 100);

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
		env.getConfig().registerTypeWithKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(BidEvent0.class, BidEvent0.BidEventKryoSerializer.class);
		env.getConfig().registerKryoType(BidEvent0.class);
		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().registerKryoType(AuctionEvent0.class);
		env.getConfig().registerKryoType(NewPersonEvent0.class);
		env.setBufferTimeout(timeout);

		env.getConfig().registerKryoType(SessionData.class);
		env.getConfig().registerTypeWithKryoSerializer(SessionData.class, SessionDataKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(SessionData.class, SessionDataKryoSerializer.class);


		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<BidEvent0> in0;
		DataStream<NewPersonEvent0> in1;
		DataStream<AuctionEvent0> in2;


		if (autogen) {

			final long bidToGenerate = params.getLong("bidToGenerate", 100_000_000);
			final int bidRate = params.getInt("bidRate", 10 * 1024 * 1024);

			final long personToGenerate = params.getLong("personToGenerate", 1_000_000);
			final long auctionsToGenerate = params.getLong("auctionsToGenerate", 100_000_000);
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


			in0 = env
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

			in0 = env
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

//			FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSourcePersons =
//				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

			FlinkKafkaConsumer011<AuctionEvent0[]> kafkaSourceAuctions =
					new FlinkKafkaConsumer011<>(AUCTIONS_TOPIC, new AuctionsDeserializationSchema(), baseCfg);

			kafkaSourceAuctions.setCommitOffsetsOnCheckpoints(true);
			kafkaSourceAuctions.setStartFromEarliest();
//			kafkaSourcePersons.setCommitOffsetsOnCheckpoints(true);
//			kafkaSourcePersons.setStartFromEarliest();

//			in1 = env
//				.addSource(kafkaSourcePersons)
//				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
//				.flatMap(new PersonsFlatMapper()).setParallelism(sourceParallelism)
//				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(2)) {
//					@Override
//					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
//						return newPersonEvent.timestamp;
//					}
//				}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {}))
//			;

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

		// query 10 - bidder session
//		DataStream<SessionOutput> sessionLatency = in0
//				.keyBy(new KeySelector<BidEvent0, Long>() {
//					@Override
//					public Long getKey(BidEvent0 value) throws Exception {
//						return value.personId;
//					}
//				})
//				.window(EventTimeSessionWindows.withGap(Time.seconds(sessionDuration)))
//				.allowedLateness(Time.seconds(sessionAllowedLateness))
//				.apply(new SessionWindowUdf())
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(1)
//				.setParallelism(windowParallelism)
//				.name("BidderSession");


		// query 4 - average price per category

		JoinHelper.UnionTypeInfo<BidEvent0, AuctionEvent0> unionType = new JoinHelper.UnionTypeInfo<>(in0.getType(), in2.getType());
//
		DataStream<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>> taggedInput1 = in0
				.map(new JoinHelper.Input1Tagger<BidEvent0, AuctionEvent0>())
				.setParallelism(in0.getParallelism())
				.returns(unionType);
		DataStream<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>> taggedInput2 = in2
				.map(new JoinHelper.Input2Tagger<BidEvent0, AuctionEvent0>())
				.setParallelism(in2.getParallelism())
				.returns(unionType);
//
		DataStream<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>> unionStream = taggedInput1.union(taggedInput2);


//		DataStream<WinningBid> winningBids = unionStream
//				.keyBy(new KeySelector<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>, Long>() {
//					@Override
//					public Long getKey(JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0> value) throws Exception {
//						return value.isOne() ? value.getOne().auctionId : value.getTwo().auctionId;
//					}
//				})
//				.process(new WinningBidsMapper())
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(1)
//				.setParallelism(windowParallelism)
//				.name("winningBids");


		DataStream<WinningBid> combo = unionStream
				.keyBy(new KeySelector<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>, Long>() {
					@Override
					public Long getKey(JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0> value) throws Exception {
						return value.isOne() ? value.getOne().auctionId : value.getTwo().auctionId;
					}
				})
				.process(new ComboQuery())
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(1)
				.setParallelism(windowParallelism)
				.name("winningBids");


//		DataStream<WinningBid> winningBids = unionStream
//				.keyBy(new KeySelector<JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>, Long>() {
//					@Override
//					public Long getKey(JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0> value) throws Exception {
//						return value.isOne() ? value.getOne().auctionId : value.getTwo().auctionId;
//					}
//				})
//				.process(new WinningBidsMapper())
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(1)
//				.setParallelism(windowParallelism)
//				.name("winningBids");

		// query 7 - highest bid

//		DataStream<SessionOutput> highestBid = in0
//				.keyBy(new KeySelector<BidEvent0, Long>() {
//					@Override
//					public Long getKey(BidEvent0 value) throws Exception {
//						return value.personId;
//					}
//				})
//				.window(EventTimeSessionWindows.withGap(Time.seconds(sessionDuration)))
//				.allowedLateness(Time.seconds(sessionAllowedLateness))
//				.apply(new SessionWindowUdf())
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(1)
//				.setParallelism(windowParallelism)
//				.name("highestBid")
////				.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
////				.process(new HighestBidProcess())
////				.setVirtualNodesNum(1)
////				.setReplicaSlotsHint(1)
			;

		// sinks
//		sessionLatency
//				.addSink(new SessionLatencyTracker("latency_session_qx"))
//				.setParallelism(windowParallelism);
//		winningBids
//				.addSink(new WinningBidLatencyTracker("winning_bid_qx"))
//				.setParallelism(windowParallelism);
//
//		highestBid.addSink(new SessionLatencyTracker("highest_bid_qx"))
//				.setParallelism(windowParallelism);

		combo
				.addSink(new WinningBidLatencyTracker("combo"))
				.setParallelism(windowParallelism);

		// q8
//		{
//			JoinHelper.UnionTypeInfo<NewPersonEvent0, AuctionEvent0> unionTypePA = new JoinHelper.UnionTypeInfo<>(in1.getType(), in2.getType());
//			DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, AuctionEvent0>> taggedInputPA1 = in1
//				.map(new JoinHelper.Input1Tagger<NewPersonEvent0, AuctionEvent0>())
//				.setParallelism(in1.getParallelism())
//				.returns(unionTypePA);
//			DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, AuctionEvent0>> taggedInputPA2 = in2
//					.map(new JoinHelper.Input2Tagger<NewPersonEvent0, AuctionEvent0>())
//					.setParallelism(in2.getParallelism())
//					.returns(unionTypePA);
//
//			DataStream<JoinHelper.TaggedUnion<NewPersonEvent0, AuctionEvent0>> unionStreamPA = taggedInputPA1.union(taggedInputPA2);
//
//			NexmarkQuery8.JoinUDF function = new NexmarkQuery8.JoinUDF();
//
//			unionStreamPA
//				.keyBy(new KeySelector<JoinHelper.TaggedUnion<NewPersonEvent0, AuctionEvent0>, Long>() {
//					@Override
//					public Long getKey(JoinHelper.TaggedUnion<NewPersonEvent0, AuctionEvent0> value) throws Exception {
//						return value.isOne() ? value.getOne().personId : value.getTwo().personId;
//					}
//				})
//				.flatMap(function)
//				.name("WindowOperator(" + windowDuration + ")")
//				.setParallelism(windowParallelism)
//				.setVirtualNodesNum(4)
//				.setReplicaSlotsHint(4)
//			.addSink(new NexmarkQuery8.NexmarkQuery8LatencyTrackingSink("latency_large_join_qx"))
//				.name("Nexmark8Sink")
//				.setParallelism(sinkParallelism);
//		}
	}

	public static class HighestBidProcess extends ProcessAllWindowFunction<SessionOutput, SessionOutput, TimeWindow>
			implements CheckpointedFunction {

		private transient MapState<Long, SessionOutput> state;

		@Override
		public void process(Context context, Iterable<SessionOutput> elements, Collector<SessionOutput> out) throws Exception {
			SessionOutput last = null;
			for (SessionOutput e : elements) {
				state.put(e.key, e);
				if (last == null) {
					last = e;
				} else if (last.latency < e.latency) {
					last = e;
				}
			}
			out.collect(last);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>("state", TypeInformation.of(Long.class), TypeInformation.of(SessionOutput.class)));
		}
	}

	public static class WinningBidsMapper
			extends KeyedProcessFunction<Long, JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>, WinningBid>
			implements CheckpointedFunction {

		private ValueState<AuctionEvent0> inFlightAuction;
		private ListState<BidEvent0> bids;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void processElement(JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0> value, Context ctx, Collector<WinningBid> out) throws Exception {
			if (value.isTwo()) {
				AuctionEvent0 auction = value.getTwo();
				if (inFlightAuction.value() == null) {
					inFlightAuction.update(auction);
					ctx.timerService().registerProcessingTimeTimer(auction.end);
				} /*else {
					LOG.warn("Duplicate auction {}", auction.auctionId);
				}*/
			} else {
				BidEvent0 event = value.getOne();
				bids.add(event);
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<WinningBid> out) throws Exception {
			super.onTimer(timestamp, ctx, out);

			long ts = Long.MIN_VALUE;
			long ingestionTs = Long.MIN_VALUE;
			for (BidEvent0 e : bids.get()) {
				if (e.timestamp > ts) {
					ts = e.timestamp;
				}
				if (e.ingestionTimestamp > ingestionTs) {
					ingestionTs = e.ingestionTimestamp;
				}
			}
			if (ts > 0) {
				out.collect(new WinningBid(ctx.getCurrentKey(), ts, ingestionTs));
			}
			bids.clear();
			inFlightAuction.update(null);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ValueStateDescriptor<AuctionEvent0> personDescriptor =
					new ValueStateDescriptor<>("inflight-auction", TypeInformation.of(AuctionEvent0.class));

			ListStateDescriptor<BidEvent0> windowContentDescriptor =
				new ListStateDescriptor<>("window-contents", TypeInformation.of(BidEvent0.class));

			inFlightAuction = context.getKeyedStateStore().getState(personDescriptor);
			bids = context.getKeyedStateStore().getListState(windowContentDescriptor);
		}
	}

	private static final Recycler<SessionData> SESSION_DATA_RECYCLER = new Recycler<SessionData>(2 * 1024 * 1024) {
		@Override
		protected SessionData newObject(Handle handle) {
			return new SessionData(handle);
		}
	};

	public static class SessionData {

		private static final int BUCKET_SIZE = 32;

		private final Recycler.Handle handle;

//		private final byte[] data;
//
//		private final ByteBuffer wrapper;

		private final BidEvent0[] objects;

		private int pos;

		public SessionData(Recycler.Handle handle) {
			this.handle = handle;
//			this.data = new byte[BUCKET_SIZE * 6 * 8];
//			this.wrapper = ByteBuffer.wrap(data);
			pos = 0;
			objects = new BidEvent0[BUCKET_SIZE];
		}

		public SessionData init() {
//			wrapper.clear();
//			wrapper.putLong(e.ingestionTimestamp);
//			wrapper.putLong(e.timestamp);
//			wrapper.putLong(e.auctionId);
//			wrapper.putLong(e.personId);
//			wrapper.putLong(e.bidId);
//			wrapper.putDouble(e.bid);
			pos = 0;
			return this;
		}

		public boolean add(BidEvent0 e) {
//			wrapper.putLong(e.ingestionTimestamp);
//			wrapper.putLong(e.timestamp);
//			wrapper.putLong(e.auctionId);
//			wrapper.putLong(e.personId);
//			wrapper.putLong(e.bidId);
//			wrapper.putDouble(e.bid);
//			return wrapper.remaining() > 0;
			objects[pos++] = e.retain();
			return pos < BUCKET_SIZE;
		}

		public void recycle() {
			for (int i = 0; i < pos; i++) {
				objects[i].recycle();
//				objects[i] = null;
			}
//			handle.recycle(this);
			SESSION_DATA_RECYCLER.recycle(this, handle);
		}

		public long getTimestamp() {
//			return wrapper.getLong(8);
			return objects[pos - 1].timestamp;
		}

		public long getIngestionTimestamp() {
//			return wrapper.getLong(0);
			return objects[pos - 1].ingestionTimestamp;
		}
	}

	public static class SessionDataKryoSerializer extends com.esotericsoftware.kryo.Serializer<SessionData> {

		public SessionDataKryoSerializer() { }

		@Override
		public void write(Kryo kryo, Output output, SessionData e) {
			for (int i = 0; i < e.pos; i++) {
				output.writeLong(e.objects[i].ingestionTimestamp);
				output.writeLong(e.objects[i].timestamp);
				output.writeLong(e.objects[i].auctionId);
				output.writeLong(e.objects[i].personId);
				output.writeLong(e.objects[i].bidId);
				output.writeDouble(e.objects[i].bid);
			}
		}

		@Override
		public SessionData read(Kryo kryo, Input input, Class<SessionData> aClass) {
//			SessionData ret = SESSION_DATA_RECYCLER.get();
//			ret.wrapper.clear();
//			input.read(ret.data);
//			ret.wrapper.position(ret.data.length);
//			return ret;
			throw new UnsupportedOperationException();
		}
	}

	public static class ComboQuery
			extends KeyedProcessFunction<Long, JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0>, WinningBid>
			implements CheckpointedFunction {

		private static final int SESSIONS_COUNT = 2;

		private transient ValueState<AuctionEvent0> inFlightAuction;
		private transient ValueState<Long> windowEnd;
		private transient ListState<BidEvent0> bids;

		private transient long seenSoFar;

		private transient ListState<SessionData>[] state;

		private transient HashMap<Long, SessionData> staging;

		private final long windowDuration = Time.hours(4).toMilliseconds();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			seenSoFar = 0;
		}

		@Override
		public void processElement(JoinHelper.TaggedUnion<BidEvent0, AuctionEvent0> value, Context ctx, Collector<WinningBid> out) throws Exception {
			if (value.isTwo()) {
				AuctionEvent0 auction = value.getTwo();
				if (inFlightAuction.value() == null) {
					final TimerService timerService = ctx.timerService();
					inFlightAuction.update(auction);
					timerService.registerEventTimeTimer(auction.end);
					long ts = timerService.currentProcessingTime() + windowDuration;
					timerService.registerProcessingTimeTimer(ts);
					windowEnd.update(ts);
				}
			} else {
				BidEvent0 event = value.getOne();
				long key = event.auctionId;
				SessionData data;
				if ((data = staging.get(key)) == null) {
					data = SESSION_DATA_RECYCLER.get().init();
					staging.put(key, data);
				}

				if (!data.add(event)) {
					for (int i = 0; i < SESSIONS_COUNT; i++) {
						state[i].add(data);
					}
					data.recycle();
					staging.remove(key);
				}

				if (staging.size() >= (8 * 8192)) {
					staging.forEach(new BiConsumer<Long, SessionData>() {
						@Override
						public void accept(Long key, SessionData s) {
							for (int i = 0; i < SESSIONS_COUNT; i++) {
								try {
									state[i].add(s);
								} catch (Exception e) {
								}
							}
							s.recycle();
						}
					});
					staging.clear();
				}
				if (seenSoFar++ % 200_000 == 0) {
					out.collect(new WinningBid(event.auctionId, event.timestamp, event.ingestionTimestamp));
				}
				bids.add(event);
//				event.recycle();
//				data.recycle();
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<WinningBid> out) throws Exception {
			super.onTimer(timestamp, ctx, out);

//			Long w = windowEnd.value();
//
//			if (w != null && timestamp > w) {
//				windowEnd.update(null);
//				bids.clear();
//				return;
//			}
//
//			long ts = Long.MIN_VALUE;
//			long ingestionTs = Long.MIN_VALUE;
//			for (SessionData e : state[0].get()) {
//				long tmp;
//				if ((tmp = e.getTimestamp()) > ts) {
//					ts = tmp;
//				}
//				if ((tmp = e.getIngestionTimestamp()) > ingestionTs) {
//					ingestionTs = tmp;
//				}
//			}
//			if (ts > 0) {
//				out.collect(new WinningBid(ctx.getCurrentKey(), ts, ingestionTs));
//				for (int i = 0; i < SESSIONS_COUNT; i++) {
//					state[i].clear();
//				}
//				inFlightAuction.update(null);
//			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			staging.forEach(new BiConsumer<Long, SessionData>() {
				@Override
				public void accept(Long key, SessionData s) {
					for (int i = 0; i < SESSIONS_COUNT; i++) {
						try {
							state[i].add(s);
						} catch (Exception e) {
						}
					}
					s.recycle();
				}
			});
			staging.clear();
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ValueStateDescriptor<AuctionEvent0> personDescriptor =
					new ValueStateDescriptor<>("inflight-auction", TypeInformation.of(AuctionEvent0.class));

			ListStateDescriptor<BidEvent0> windowContentDescriptor =
				new ListStateDescriptor<>("window-contents", TypeInformation.of(BidEvent0.class));

			ValueStateDescriptor<Long> timerDescriptor =
					new ValueStateDescriptor<>("inflight-timer", TypeInformation.of(Long.class));

			inFlightAuction = context.getKeyedStateStore().getState(personDescriptor);
			windowEnd = context.getKeyedStateStore().getState(timerDescriptor);
			bids = context.getKeyedStateStore().getListState(windowContentDescriptor);

			state = new ListState[SESSIONS_COUNT];
			for (int i = 0; i < SESSIONS_COUNT; i++) {
				ListStateDescriptor<SessionData> sessionDescriptor =
						new ListStateDescriptor<>("window-session-" + i, TypeInformation.of(SessionData.class));
				state[i] = context.getKeyedStateStore().getListState(sessionDescriptor);
			}

			staging = new HashMap<>(1024);
		}
	}

	public static class SessionWindowUdf extends RichWindowFunction<BidEvent0, SessionOutput, Long, TimeWindow> {

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void apply(Long key, TimeWindow window, Iterable<BidEvent0> input, Collector<SessionOutput> out) throws Exception {
			long latency = Long.MIN_VALUE;
			long ingestionLatency = Long.MIN_VALUE;
			for (BidEvent0 e : input) {
				if (e.timestamp > latency) {
					latency = e.timestamp;
				}
				if (e.ingestionTimestamp > ingestionLatency) {
					ingestionLatency = e.ingestionTimestamp;
				}
			}
			out.collect(new SessionOutput(key, latency, ingestionLatency));
		}
	}

	public static class WinningBid implements Serializable {

		private final long key, latency, ingestionLatency;

		public WinningBid(Long currentKey, long latency, long ingestionLatency) {
			this.key = currentKey;
			this.latency = latency;
			this.ingestionLatency = ingestionLatency;
		}
	}

	public static class SessionOutput implements Serializable {

		public final long key, latency, ingestionLatency;

		public SessionOutput(long key, long latency, long ingestionLatency) {
			this.key = key;
			this.latency = latency;
			this.ingestionLatency = ingestionLatency;
		}
	}
//
//	public static final class SessionWindowAggregator implements AggregateFunction<BidEvent0, SessionWindowAccumulator, SessionOutput> {
//
//		@Override
//		public SessionWindowAccumulator createAccumulator() {
//			return new SessionWindowAccumulator();
//		}
//
//		@Override
//		public SessionWindowAccumulator add(BidEvent0 value, SessionWindowAccumulator accumulator) {
//			return null;
//		}
//
//		@Override
//		public SessionOutput getResult(SessionWindowAccumulator accumulator) {
//			return null;
//		}
//
//		@Override
//		public SessionWindowAccumulator merge(SessionWindowAccumulator a, SessionWindowAccumulator b) {
//			return null;
//		}
//	}


	private static final class SessionLatencyTracker extends RichSinkFunction<SessionOutput> {

		private static final long LATENCY_THRESHOLD = 10L * 60L * 1000L;

		private transient SummaryStatistics sinkLatencyBid;
		private transient SummaryStatistics sinkLatencyFlightTime;

		private transient BufferedWriter writer;

		private transient StringBuffer stringBuffer;

		private transient int index;

		private transient Thread cleaningHelper;

		private transient boolean logInit = false;

		private transient int writtenSoFar = 0;

		private final String name;

		public SessionLatencyTracker(String name) {
			this.name = name;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.sinkLatencyBid = new SummaryStatistics();
			this.sinkLatencyFlightTime = new SummaryStatistics();
			this.stringBuffer = new StringBuffer(2048);
			this.index = getRuntimeContext().getIndexOfThisSubtask();

			File logDir = new File(readProperty("flink.sink.csv.dir", System.getProperty("java.io.tmpdir")));
			File logSubDir = new File(logDir, name + "_" + + index);

			if (!logSubDir.exists()) {
				logSubDir.mkdirs();
			}

			File logFile = new File(logSubDir, name + "_" + + index + ".csv");

			if (logFile.exists()) {
				this.writer = new BufferedWriter(new FileWriter(logFile, true));
				this.writer.write("\n");
			} else {
				this.writer = new BufferedWriter(new FileWriter(logFile, false));
				stringBuffer.append("subtask,ts,bidLatencyCount,flightTimeCount,bidLatencyMean,flightTimeMean,bidLatencyMin,flightTimeMin,bidLatencyMax,flightTimeMax");
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

//				stringBuffer.append(sinkLatencyBid.getStandardDeviation());
//				stringBuffer.append(",");
//				stringBuffer.append(sinkLatencyFlightTime.getStandardDeviation());
//				stringBuffer.append(",");append


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
		public void invoke(SessionOutput record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			long latency = timeMillis - record.latency;
			if (latency <= LATENCY_THRESHOLD) {
				sinkLatencyBid.addValue(latency);
				sinkLatencyFlightTime.addValue(timeMillis - record.ingestionLatency);
				updateCSV(timeMillis);
			}
		}
	}

	private static final class WinningBidLatencyTracker extends RichSinkFunction<WinningBid> {

		private static final long LATENCY_THRESHOLD = 10L * 60L * 1000L;

		private transient SummaryStatistics sinkLatencyBid;
		private transient SummaryStatistics sinkLatencyFlightTime;

		private transient BufferedWriter writer;

		private transient StringBuffer stringBuffer;

		private transient int index;

		private transient Thread cleaningHelper;

		private transient boolean logInit = false;

		private transient int writtenSoFar = 0;

		private final String name;

//		private transient long seenSoFar = 0;

		private transient AtomicDouble latency;

		public WinningBidLatencyTracker(String name) {
			this.name = name;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.sinkLatencyBid = new SummaryStatistics();
			this.sinkLatencyFlightTime = new SummaryStatistics();
			this.stringBuffer = new StringBuffer(2048);
			this.index = getRuntimeContext().getIndexOfThisSubtask();

			File logDir = new File(readProperty("flink.sink.csv.dir", System.getProperty("java.io.tmpdir")));
			File logSubDir = new File(logDir, name + "_" + index);
			if (!logSubDir.exists()) {
				logSubDir.mkdirs();
			}
			File logFile = new File(logSubDir, name + "_" + index + ".csv");

			if (logFile.exists()) {
				this.writer = new BufferedWriter(new FileWriter(logFile, true));
				this.writer.write("\n");
			} else {
				this.writer = new BufferedWriter(new FileWriter(logFile, false));
				stringBuffer.append("subtask,ts,bidLatencyCount,flightTimeCount,bidLatencyMean,flightTimeMean,bidLatencyStd,flightTimeStd,bidLatencyMin,flightTimeMin,bidLatencyMax,flightTimeMax");
				stringBuffer.append("\n");
				writer.write(stringBuffer.toString());
				writtenSoFar += stringBuffer.length() * 2;
			}

			cleaningHelper = ShutdownHookUtil.addShutdownHook(writer, getRuntimeContext().getTaskNameWithSubtasks(), LOG);

			stringBuffer.setLength(0);
			logInit = true;
//			seenSoFar = 0;
			latency = new AtomicDouble(0);

			getRuntimeContext().getMetricGroup().gauge("latency", new Gauge<Double>() {
				@Override
				public Double getValue() {
					return latency.get();
				}
			});
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
		public void invoke(WinningBid record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			long latency = timeMillis - record.latency;
			if (latency <= LATENCY_THRESHOLD) {
				sinkLatencyBid.addValue(latency);
				sinkLatencyFlightTime.addValue(timeMillis - record.ingestionLatency);
				updateCSV(timeMillis);
				this.latency.lazySet(sinkLatencyBid.getMean());
			}
		}
	}

}
