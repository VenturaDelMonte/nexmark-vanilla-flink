package io.ventura.nexmark.generator;

import akka.routing.MurmurHash;
import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5;
import io.ventura.nexmark.beans.NexmarkEvent;
import io.ventura.nexmark.beans.Serializer;
import io.ventura.nexmark.common.NexmarkCommon;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import scala.util.hashing.MurmurHash3;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class GeneratorPipeline {
	
	public static void runGenerator(StreamExecutionEnvironment env, ParameterTool params) {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final long numEvents = params.getLong("numEvents", 1);
		final long genSpeedMin = params.getLong("genSpeedMin", 1_000);
		final long genSpeedMax = params.getLong("genSpeedMax", genSpeedMin + 1);
		final String kafkaServers = params.get("kafkaServers", "localhost:9092");

		Properties kafkaCfg = new Properties();

		kafkaCfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		kafkaCfg.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + (4 * 1024 * 1024));
		kafkaCfg.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "32768");
		kafkaCfg.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "im-job");
		kafkaCfg.setProperty("offsets.commit.timeout.ms", "" + (3 * 60 * 1000));
		kafkaCfg.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "" + (10 * 1024 * 1024));
		kafkaCfg.setProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
		kafkaCfg.put(ProducerConfig.ACKS_CONFIG, "0");
		kafkaCfg.put(ProducerConfig.LINGER_MS_CONFIG, "100");
		kafkaCfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 256 * 1024 * 1024);
		kafkaCfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

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
		env.getConfig().registerTypeWithKryoSerializer(NexmarkEvent.AuctionEvent.class, Serializer.AuctionEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NexmarkEvent.AuctionEvent.class, Serializer.AuctionEventKryoSerializer.class);
		env.getConfig().registerKryoType(NexmarkEvent.AuctionEvent.class);
		env.getConfig().registerTypeWithKryoSerializer(NexmarkEvent.PersonEvent.class, Serializer.NewPersonEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NexmarkEvent.PersonEvent.class, Serializer.NewPersonEventKryoSerializer.class);
		env.getConfig().registerKryoType(NexmarkEvent.PersonEvent.class);
		env.getConfig().registerTypeWithKryoSerializer(NexmarkEvent.BidEvent.class, Serializer.BidEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NexmarkEvent.BidEvent.class, Serializer.BidEventKryoSerializer.class);
		env.getConfig().registerKryoType(NexmarkEvent.BidEvent.class);
		env.getConfig().registerTypeWithKryoSerializer(NexmarkQuery5.NexmarkQuery4Accumulator.class, NexmarkQuery5.NexmarkQuery4AccumulatorSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NexmarkQuery5.NexmarkQuery4Accumulator.class, NexmarkQuery5.NexmarkQuery4AccumulatorSerializer.class);
		env.getConfig().registerKryoType(NexmarkQuery5.NexmarkQuery4Accumulator.class);
		env.getConfig().enableObjectReuse();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


		DataStream<NexmarkEvent> in = env.addSource(new RichParallelSourceFunction<NexmarkEvent>() {

			private volatile boolean running = true;

			private RateLimiter limiter;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				limiter = RateLimiter.create(ThreadLocalRandom.current().nextLong(genSpeedMin, genSpeedMax));
			}

			@Override
			public void run(SourceContext<NexmarkEvent> ctx) throws Exception {
				NexmarkEvent e;
				long auctionParam[] = new long[2];
				long bidParam[] = new long[2];
				long personParam[] = new long[2];
				NexmarkCommon.geAuctiontride(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getMaxNumberOfParallelSubtasks(), auctionParam);
				NexmarkCommon.getPersonStride(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getMaxNumberOfParallelSubtasks(), personParam);
				NexmarkCommon.getBidStride(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getMaxNumberOfParallelSubtasks(), bidParam);

				for (long i = 0; i < numEvents && running;) {
					synchronized (ctx.getCheckpointLock()) {
						long timestamp = System.currentTimeMillis();
						ThreadLocalRandom r = ThreadLocalRandom.current();
						for (int j = 0; j < 10; j++, i++) {
							long epoch = i / NexmarkCommon.TOTAL_EVENT_RATIO;
							long offset = i % NexmarkCommon.TOTAL_EVENT_RATIO;
							if (offset < NexmarkCommon.PERSON_EVENT_RATIO) {
								long personId = personParam[0] + epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset;
								ctx.collect(new NexmarkEvent.PersonEvent(personId, timestamp, r));
							} else if (offset < (NexmarkCommon.PERSON_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO)) {
								if (offset < NexmarkCommon.PERSON_EVENT_RATIO) {
									epoch--;
									offset = NexmarkCommon.AUCTION_EVENT_RATIO - 1;
								} else {
									offset = NexmarkCommon.AUCTION_EVENT_RATIO - 1;
								}
								long auctionId = auctionParam[0] + epoch * NexmarkCommon.AUCTION_EVENT_RATIO + offset;//r.nextLong(minAuctionId, maxAuctionId);

								if (offset >= NexmarkCommon.PERSON_EVENT_RATIO) {
									offset = NexmarkCommon.PERSON_EVENT_RATIO - 1;
								}
								long matchingPerson;
								if (r.nextInt(100) > 85) {
									long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset;
									matchingPerson = auctionParam[0] + (personId / NexmarkCommon.HOT_SELLER_RATIO) * NexmarkCommon.HOT_SELLER_RATIO;
								} else {
									long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset + 1;
									long activePersons = Math.min(personId, 20_000);
									long n = r.nextLong(activePersons + 100);
									matchingPerson = auctionParam[0] + personId + activePersons - n;
								}
								ctx.collect(NexmarkEvent.AUCTIONS_RECYCLER.get().initEx(auctionId, matchingPerson, timestamp, timestamp + 10_000, r));
							} else {
								long auction, bidder;
								if (r.nextInt(100) > NexmarkCommon.HOT_AUCTIONS_PROB) {
									auction = auctionParam[0] + (((epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1) / NexmarkCommon.HOT_AUCTION_RATIO) * NexmarkCommon.HOT_AUCTION_RATIO);
								} else {
									long a = Math.max(0, epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1 - 20_000);
									long b = epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1;
									auction = auctionParam[0] + a + r.nextLong(b - a + 1 + 100);
								}

								if (r.nextInt(100) > 85) {
									long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + NexmarkCommon.PERSON_EVENT_RATIO - 1;
									bidder = personParam[0] + (personId / NexmarkCommon.HOT_SELLER_RATIO) * NexmarkCommon.HOT_SELLER_RATIO;
								} else {
									long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + NexmarkCommon.PERSON_EVENT_RATIO - 1;
									long activePersons = Math.min(personId, 60_000);
									long n = r.nextLong(activePersons + 100);
									bidder = personParam[0] + personId + activePersons - n;
								}

								ctx.collect(NexmarkEvent.BIDS_RECYCLER.get().initEx(timestamp, timestamp, Math.abs(auction), Math.abs(bidder), -1, r.nextDouble(10_000_000)));
							}
						}
					}
					limiter.acquire(10);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		}).setParallelism(sourceParallelism);

		FlinkKafkaProducer011<NexmarkEvent> kafkaProducer = new FlinkKafkaProducer011<>(
				"nexmark-events",
				new Serializer.KafkaSerializationSchema(),
				kafkaCfg,
				Optional.of(new FlinkKafkaPartitioner<NexmarkEvent>() {

//					private int parallelInstanceId;
//					private int parallelInstances;
//
//					@Override
//					public void open(int parallelInstanceId, int parallelInstances) {
//						super.open(parallelInstanceId, parallelInstances);
//						this.parallelInstanceId = parallelInstanceId;
//						this.parallelInstances = parallelInstances;
//					}

					@Override
					public int partition(NexmarkEvent nexmarkEvent, byte[] key, byte[] value, String topic, int[] partitions) {
						return hash32(key, 0, 4, 104729) % partitions.length;
					}
				}));

		kafkaProducer.setWriteTimestampToKafka(false);
		kafkaProducer.ignoreFailuresAfterTransactionTimeout();
		kafkaProducer.setLogFailuresOnly(false);

		in.addSink(kafkaProducer);
	}

	// MurmurHash3 taken from Hive codebase

	public static int hash32(byte[] data, final int offset, final int length, final int seed) {
		int hash = seed;
		final int nblocks = length >> 2;

		// body
		for (int i = 0; i < nblocks; i++) {
			int i_4 = i << 2;
			int k = (data[offset + i_4] & 0xff)
					| ((data[offset + i_4 + 1] & 0xff) << 8)
					| ((data[offset + i_4 + 2] & 0xff) << 16)
					| ((data[offset + i_4 + 3] & 0xff) << 24);

			hash = mix32(k, hash);
		}

		// tail
		int idx = nblocks << 2;
		int k1 = 0;
		switch (length - idx) {
			case 3:
				k1 ^= data[offset + idx + 2] << 16;
			case 2:
				k1 ^= data[offset + idx + 1] << 8;
			case 1:
				k1 ^= data[offset + idx];

				// mix functions
				k1 *= 0xcc9e2d51;
				k1 = Integer.rotateLeft(k1, 15);
				k1 *= 0x1b873593;
				hash ^= k1;
		}

		return fmix32(length, hash);
	}

	private static int mix32(int k, int hash) {
		k *= 0xcc9e2d51;
		k = Integer.rotateLeft(k, 15);
		k *= 0x1b873593;
		hash ^= k;
		return Integer.rotateLeft(hash, 13) * 5 + 0xe6546b64;
	}

	private static int fmix32(int length, int hash) {
		hash ^= length;
		hash ^= (hash >>> 16);
		hash *= 0x85ebca6b;
		hash ^= (hash >>> 13);
		hash *= 0xc2b2ae35;
		hash ^= (hash >>> 16);

		return hash;
	}
}
