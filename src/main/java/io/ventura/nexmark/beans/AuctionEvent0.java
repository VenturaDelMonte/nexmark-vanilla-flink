package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import javax.annotation.Nonnegative;
import java.io.Serializable;

/**
 * Class needs public field with default, no-argument constructor to be serializable.
 */
public class AuctionEvent0 implements Serializable {

//    private static final Logger LOG = LoggerFactory.getLogger(AuctionEvent.class);


    public long timestamp;
    public long auctionId;
    public long personId;
    public long itemId;
    public double initialPrice;
    public long start;
    public long end;
    public long categoryId;
    public long ingestionTimestamp;

    public AuctionEvent0() {
//        LOG.debug("Created person event with default constructor");
    }

    public AuctionEvent0(long timestamp, long auctionId, long itemId, long personId, double initialPrice, long categoryID, long start, long end) {
        this(timestamp, auctionId, itemId, personId, initialPrice, categoryID, start, end, System.currentTimeMillis());
    }

    public AuctionEvent0(
    		@Nonnegative long timestamp,
			@Nonnegative long auctionId,
			@Nonnegative long itemId,
			@Nonnegative long personId,
			@Nonnegative double initialPrice,
			@Nonnegative long categoryID,
			@Nonnegative long start,
			@Nonnegative long end,
			@Nonnegative long ingestionTimestamp) {
//        LOG.debug("Created person event with auctionId {} and personId {}", auctionId, personId);

        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.itemId = itemId;
        this.initialPrice = initialPrice;
        this.categoryId = categoryID;
        this.start = start;
        this.end = end;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public long getAuctionId() {
        return auctionId;
    }

    public Long getPersonId() {
        return personId;
    }

    public long getItemId() {
        return itemId;
    }

    public Double getInitialPrice() {
        return initialPrice;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public long getIngestionTimestamp() {
        return ingestionTimestamp;
    }


    public static class AuctionEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<AuctionEvent0> {

    	public AuctionEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, AuctionEvent0 event) {
			output.writeLong(event.timestamp);
			output.writeLong(event.auctionId);
			output.writeLong(event.itemId);
			output.writeLong(event.personId);
			output.writeDouble(event.initialPrice);
			output.writeLong(event.start);
			output.writeLong(event.end);
			output.writeLong(event.categoryId);
			output.writeLong(event.ingestionTimestamp);
		}

		@Override
		public AuctionEvent0 read(Kryo kryo, Input input, Class<AuctionEvent0> aClass) {
			Long timestamp = input.readLong();
			long auctionId = input.readLong();
			long itemId = input.readLong();
			long personId = input.readLong();
			Double initialPrice = input.readDouble();
			Long start = input.readLong();
			Long end = input.readLong();
			Long categoryId = input.readLong();
			Long ingestionTimestamp = input.readLong();

			return new AuctionEvent0(timestamp, auctionId, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
		}
}

}
