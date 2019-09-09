package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.BidEvent0;
import io.ventura.nexmark.common.NexmarkCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class NexmarkBidSource extends RichParallelSourceFunction<BidEvent0> {


	private final long recordsToGenerate, recordsPerSecond;
	private long minAuctionId;
	private long minPersonId;

	private volatile boolean shouldContinue = true;

	private final int MINI_BATCH = 5;

	public NexmarkBidSource(long recordsToGenerate, int recordsPerSecond) {
		this.recordsToGenerate = recordsToGenerate;
		this.recordsPerSecond = recordsPerSecond;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		minAuctionId = NexmarkCommon.START_ID_AUCTION[getRuntimeContext().getIndexOfThisSubtask()];
		minPersonId = NexmarkCommon.START_ID_PERSON[getRuntimeContext().getIndexOfThisSubtask()];
	}

	@Override
	public void run(SourceContext<BidEvent0> ctx) throws Exception {
		ThreadLocalRandom r = ThreadLocalRandom.current();
		final RateLimiter limiter = RateLimiter.create(recordsPerSecond);
		for (long eventId = 0; eventId < recordsToGenerate && shouldContinue; ) {
			long timestamp = System.currentTimeMillis();
			synchronized (ctx.getCheckpointLock()) {
				for (int i = 0; i < MINI_BATCH; i++, eventId++) {
					long auction, bidder;

					long epoch = eventId / NexmarkCommon.TOTAL_EVENT_RATIO;
					long offset = eventId % NexmarkCommon.TOTAL_EVENT_RATIO;

					if (r.nextInt(100) > NexmarkCommon.HOT_AUCTIONS_PROB) {
						auction = minAuctionId + (((epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1) / NexmarkCommon.HOT_AUCTION_RATIO) * NexmarkCommon.HOT_AUCTION_RATIO);
					} else {
						long a = Math.max(0, epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1 - 20_000);
						long b = epoch * NexmarkCommon.AUCTION_EVENT_RATIO + NexmarkCommon.AUCTION_EVENT_RATIO - 1;
						auction = minAuctionId + a + r.nextLong(b - a + 1 + 100);
					}

					if (r.nextInt(100) > 85) {
						long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + NexmarkCommon.PERSON_EVENT_RATIO - 1;
						bidder = minPersonId + (personId / NexmarkCommon.HOT_SELLER_RATIO) * NexmarkCommon.HOT_SELLER_RATIO;
					} else {
						long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + NexmarkCommon.PERSON_EVENT_RATIO - 1;
						long activePersons = Math.min(personId, 60_000);
						long n = r.nextLong(activePersons + 100);
						bidder = minPersonId + personId + activePersons - n;
					}

					ctx.collect(new BidEvent0(timestamp, timestamp, Math.abs(auction), Math.abs(bidder), -1, r.nextDouble(10_000_000)));
				}
			}
			limiter.acquire(MINI_BATCH);
		}
	}

	@Override
	public void cancel() {
		shouldContinue = false;
	}
}
