package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.ventura.nexmark.original.RandomStrings;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

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

    private String name;
    private String descr;

    public AuctionEvent0() {
//        LOG.debug("Created person event with default constructor");
    }

    public AuctionEvent0(long timestamp, long auctionId, String name, String descr, long itemId, long personId, double initialPrice, long categoryID, long start, long end) {
        this(timestamp, auctionId, name, descr, itemId, personId, initialPrice, categoryID, start, end, System.currentTimeMillis());
    }

    public AuctionEvent0(
    		@Nonnegative long timestamp,
			@Nonnegative long auctionId,
			@Nonnull String name,
			@Nonnull String descr,
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
        this.name = name;
        this.descr = descr;
        this.categoryId = categoryID;
        this.start = timestamp;
        this.end = timestamp;
        this.ingestionTimestamp = ingestionTimestamp;
    }

	public AuctionEvent0(long auctionId, long matchingPerson, long timestamp, long end, ThreadLocalRandom r) {
		this.auctionId = auctionId;
		this.personId = matchingPerson;
		this.ingestionTimestamp = this.timestamp = timestamp;
		this.start = timestamp;
		this.end = end;
		this.name = new String(RandomStrings.RANDOM_STRINGS_NAME[r.nextInt(RandomStrings.RANDOM_STRINGS_NAME.length)]);
		this.descr = new String(RandomStrings.RANDOM_STRINGS_DESCR[r.nextInt(RandomStrings.RANDOM_STRINGS_DESCR.length)]);
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

    public String getName() {
    	return name;
	}

	public String getDescr() {
		return descr;
	}

	public static class AuctionEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<AuctionEvent0> {

    	public AuctionEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, AuctionEvent0 event) {
			output.writeLong(event.timestamp);
			output.writeLong(event.auctionId);
			output.writeLong(event.itemId);
			output.writeString(event.name);
			output.writeString(event.descr);
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
			String name = input.readString();
			String descr = input.readString();
			long personId = input.readLong();
			Double initialPrice = input.readDouble();
			Long start = input.readLong();
			Long end = input.readLong();
			Long categoryId = input.readLong();
			Long ingestionTimestamp = input.readLong();

			return new AuctionEvent0(timestamp, auctionId, name, descr, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
		}
}

}
