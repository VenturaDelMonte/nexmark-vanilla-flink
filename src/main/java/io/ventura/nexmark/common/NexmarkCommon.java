package io.ventura.nexmark.common;

public class NexmarkCommon {

	public static final String PERSONS_TOPIC = "nexmark_persons";
	public static final String AUCTIONS_TOPIC = "nexmark_auctions";


	public static final long PERSON_EVENT_RATIO = 1;
	public static final long AUCTION_EVENT_RATIO = 4;
	public static final long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO;

	public static final int MAX_PARALLELISM = 50;

	public static final long START_ID_AUCTION[] = new long[MAX_PARALLELISM];
	public static final long START_ID_PERSON[] = new long[MAX_PARALLELISM];

	public static final long MAX_PERSON_ID = 540_000_000L;
	public static final long MAX_AUCTION_ID = 540_000_000_000L;

	public static final int HOT_SELLER_RATIO = 100;


	static {

		START_ID_AUCTION[0] = START_ID_PERSON[0] = 0;

		long person_stride = MAX_PERSON_ID / MAX_PARALLELISM;
		long auction_stride = MAX_AUCTION_ID / MAX_PARALLELISM;
		for (int i = 1; i < MAX_PARALLELISM; i++) {
			START_ID_PERSON[i] = START_ID_PERSON[i - 1] + person_stride;
			START_ID_AUCTION[i] = START_ID_AUCTION[i - 1] + auction_stride;
		}


	}

}
