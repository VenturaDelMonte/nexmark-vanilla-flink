package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.common.NexmarkCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class NexmarkAuctionSource extends RichParallelSourceFunction<AuctionEvent0> {

	private final long recordsToGenerate, recordsPerSecond;
	private long minAuctionId;
	private long minPersonId;

	private volatile boolean shouldContinue = true;

	private final int MINI_BATCH = 5;

	public NexmarkAuctionSource(long recordsToGenerate, int recordsPerSecond) {
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
	public void run(SourceContext<AuctionEvent0> ctx) throws Exception {
		ThreadLocalRandom r = ThreadLocalRandom.current();
		final RateLimiter limiter = RateLimiter.create(recordsPerSecond);
		for (long eventId = 0; eventId < recordsToGenerate && shouldContinue; ) {
			long timestamp = System.currentTimeMillis();
			synchronized (ctx.getCheckpointLock()) {
				for (int i = 0; i < MINI_BATCH; i++, eventId++) {
					long epoch = eventId / NexmarkCommon.TOTAL_EVENT_RATIO;
					long offset = eventId % NexmarkCommon.TOTAL_EVENT_RATIO;
					if (offset < NexmarkCommon.PERSON_EVENT_RATIO) {
						epoch--;
						offset = NexmarkCommon.AUCTION_EVENT_RATIO - 1;
					} else {
						offset = NexmarkCommon.AUCTION_EVENT_RATIO - 1;
					}
					long auctionId = minAuctionId + epoch * NexmarkCommon.AUCTION_EVENT_RATIO + offset;//r.nextLong(minAuctionId, maxAuctionId);

					epoch = eventId / NexmarkCommon.TOTAL_EVENT_RATIO;
					offset = eventId % NexmarkCommon.TOTAL_EVENT_RATIO;

					if (offset >= NexmarkCommon.PERSON_EVENT_RATIO) {
						offset = NexmarkCommon.PERSON_EVENT_RATIO - 1;
					}
					long matchingPerson;
					if (r.nextInt(100) > 85) {
						long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset;
						matchingPerson = minPersonId + (personId / NexmarkCommon.HOT_SELLER_RATIO) * NexmarkCommon.HOT_SELLER_RATIO;
					} else {
						long personId = epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset + 1;
						long activePersons = Math.min(personId, 20_000);
						long n = r.nextLong(activePersons + 100);
						matchingPerson = minPersonId + personId + activePersons - n;
					}
					ctx.collect(new AuctionEvent0(auctionId, matchingPerson, timestamp, timestamp + 120_000, r));
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