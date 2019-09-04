package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.AuctionEvent0;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AuctionsDeserializationSchema implements KeyedDeserializationSchema<AuctionEvent0[]> {

	private static final int AUCTION_RECORD_SIZE = 269;

	private static final TypeInformation<AuctionEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<AuctionEvent0[]>() {});

//		private final long bytesToRead;

//		private long bytesReadSoFar;

//		private long lastBacklog = Long.MAX_VALUE;


	private boolean isPartitionConsumed = false;

	public AuctionsDeserializationSchema() {
//			this.bytesToRead = (bytesToRead / AUCTION_RECORD_SIZE) * AUCTION_RECORD_SIZE;
//			this.bytesReadSoFar = 0;
	}

	@Override
	public AuctionEvent0[] deserialize(
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

		Preconditions.checkArgument(checksum == 0x30061992);
		Preconditions.checkArgument(((8192 - 16) / AUCTION_RECORD_SIZE) >= itemsInThisBuffer);

		AuctionEvent0[] data = new AuctionEvent0[itemsInThisBuffer];
		long ingestionTimestamp = System.currentTimeMillis();

		byte[] tmp0 = new byte[20];
		byte[] tmp1 = new byte[200];

		for (int i = 0; i < data.length; i++) {
			long id = wrapper.getLong();
			long pid = wrapper.getLong();
			byte c = wrapper.get();
			int itemId = wrapper.getInt();
			long start = wrapper.getLong();
			long end = wrapper.getLong();
			int price = wrapper.getInt();
			wrapper.get(tmp0);
			wrapper.get(tmp1);
			long ts = wrapper.getLong();
//				Preconditions.checkArgument(ts > 0);
			data[i] = new AuctionEvent0(
					ts,
					id,
					new String(tmp0),
					new String(tmp1),
					itemId,
					pid,
					(double) price,
					c,
					start,
					end,
					ingestionTimestamp);
		}

//			bytesReadSoFar += buffer.length;
//			Preconditions.checkArgument(newBacklog < lastBacklog, "newBacklog: %s oldBacklog: %s", newBacklog, lastBacklog);
//			lastBacklog = newBacklog;
		isPartitionConsumed = newBacklog <= itemsInThisBuffer;
		return data;
	}

	@Override
	public boolean isEndOfStream(AuctionEvent0[] nextElement) {
		return isPartitionConsumed;
	}

	@Override
	public TypeInformation<AuctionEvent0[]> getProducedType() {
		return FLINK_INTERNAL_TYPE;
	}
}