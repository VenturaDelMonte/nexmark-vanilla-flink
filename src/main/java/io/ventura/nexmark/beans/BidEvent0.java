package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class BidEvent0 implements Serializable {

    public final long ingestionTimestamp;
    public final long timestamp;
    public final long auctionId;
    public final long personId;
    public final long bidId;
    public final double bid;


    public BidEvent0(long ingestionTimestamp, long timestamp, long auctionId, long personId, long bidId, double bid) {
        this.ingestionTimestamp = ingestionTimestamp;
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.bidId = bidId;
        this.bid = bid;
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
        }

        @Override
        public BidEvent0 read(Kryo kryo, Input input, Class<BidEvent0> aClass) {
            long ingestionTimestamp = input.readLong();
            long timestamp = input.readLong();
            long auctionId = input.readLong();
            long personId = input.readLong();
            long bidId = input.readLong();
            double bid = input.readDouble();
            return new BidEvent0(ingestionTimestamp, timestamp, auctionId, personId, bidId, bid);
        }


    }

}