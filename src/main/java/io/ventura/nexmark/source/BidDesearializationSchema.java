package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.BidEvent0;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BidDesearializationSchema implements KeyedDeserializationSchema<BidEvent0[]> {

	private static final TypeInformation<BidEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<BidEvent0[]>() {});

	private boolean isPartitionConsumed = false;

	private final static int BID_RECORD_SIZE = 8 + 8 + 8 + 8;


	@Override
	public BidEvent0[] deserialize(
			byte[] messageKey,
			byte[] buffer,
			String topic,
			int partition,
			long offset) throws IOException {

		Preconditions.checkArgument(buffer.length == 8192);

		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		int checksum = wrapper.getInt();
		int itemsInThisBuffer = wrapper.getInt();
		long newBacklog = wrapper.getLong();

		Preconditions.checkArgument(checksum == 0xdeedbeaf);
		Preconditions.checkArgument(((8192 - 16) / BID_RECORD_SIZE) >= itemsInThisBuffer);

		BidEvent0[] data = new BidEvent0[itemsInThisBuffer];
		long ingestionTimestamp = System.currentTimeMillis();

		for (int i = 0; i < data.length; i++) {

			long bidderId = wrapper.getLong();
			long auctionId = wrapper.getLong();
			double price = wrapper.getDouble();
			long timestamp = wrapper.getLong();
			

			data[i] = new BidEvent0(ingestionTimestamp, timestamp, auctionId, bidderId, -1, price);
		}

		isPartitionConsumed = newBacklog <= itemsInThisBuffer;
		return data;
	}

	@Override
	public boolean isEndOfStream(BidEvent0[] bidEvent0) {
		return isPartitionConsumed;
	}

	@Override
	public TypeInformation<BidEvent0[]> getProducedType() {
		return FLINK_INTERNAL_TYPE;
	}
}
