package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.shaded.netty4.io.netty.util.Recycler;

import java.io.Serializable;

public class BidEvent0 implements Serializable {


    private static final Recycler<BidEvent0> BIDS_RECYCLER = new Recycler<BidEvent0>(512 * 1024 * 1024) {
        @Override
        protected BidEvent0 newObject(Handle handle) {
            return new BidEvent0(handle);
        }
    };

    public long ingestionTimestamp;
    public long timestamp;
    public long auctionId;
    public long personId;
    public long bidId;
    public double bid;

    private final Recycler.Handle handle;

    public BidEvent0(Recycler.Handle handle) {
        this.handle = handle;
    }

    public BidEvent0 init(long ingestionTimestamp, long timestamp, long auctionId, long personId, long bidId, double bid) {
        this.ingestionTimestamp = ingestionTimestamp;
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.bidId = bidId;
        this.bid = bid;

        return this;
    }


    public static class BidEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<BidEvent0> {

    	public BidEventKryoSerializer() {

		}

        @Override
        public void write(Kryo kryo, Output output, BidEvent0 bidEvent0) {
            output.writeLong(bidEvent0.ingestionTimestamp);
            output.writeLong(bidEvent0.timestamp);
            output.writeLong(bidEvent0.auctionId);
            output.writeLong(bidEvent0.personId);
            output.writeLong(bidEvent0.bidId);
            output.writeDouble(bidEvent0.bid);

            BIDS_RECYCLER.recycle(bidEvent0, bidEvent0.handle);
        }

        @Override
        public BidEvent0 read(Kryo kryo, Input input, Class<BidEvent0> aClass) {
            long ingestionTimestamp = input.readLong();
            long timestamp = input.readLong();
            long auctionId = input.readLong();
            long personId = input.readLong();
            long bidId = input.readLong();
            double bid = input.readDouble();
            return BIDS_RECYCLER.get().init(ingestionTimestamp, timestamp, auctionId, personId, bidId, bid);
        }


    }

}