package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.common.NexmarkCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class NexmarkPersonSource extends RichParallelSourceFunction<NewPersonEvent0> {

	private long minPersonId;
	private final long recordsToGenerate, recordsPerSecond;

	private volatile boolean shouldContinue = true;

	private final int MINI_BATCH = 5;

	public NexmarkPersonSource(long recordsToGenerate, int recordsPerSecond) {
		this.recordsToGenerate = recordsToGenerate;
		this.recordsPerSecond = recordsPerSecond;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		minPersonId = NexmarkCommon.START_ID_PERSON[getRuntimeContext().getIndexOfThisSubtask()];
	}

	@Override
	public void run(SourceContext<NewPersonEvent0> ctx) throws Exception {
		ThreadLocalRandom r = ThreadLocalRandom.current();
		final RateLimiter limiter = RateLimiter.create(recordsPerSecond);
		for (long eventId = 0; eventId < recordsToGenerate && shouldContinue; ) {
			synchronized (ctx.getCheckpointLock()) {
				long timestamp = System.currentTimeMillis();
				for (int i = 0; i < MINI_BATCH; i++, eventId++) {
					long epoch = eventId / NexmarkCommon.TOTAL_EVENT_RATIO;
					long offset = eventId % NexmarkCommon.TOTAL_EVENT_RATIO;
					if (offset >= NexmarkCommon.PERSON_EVENT_RATIO) {
						offset = NexmarkCommon.PERSON_EVENT_RATIO - 1;
					}
					long personId = minPersonId + epoch * NexmarkCommon.PERSON_EVENT_RATIO + offset;

					ctx.collect(new NewPersonEvent0(personId, timestamp, r));
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