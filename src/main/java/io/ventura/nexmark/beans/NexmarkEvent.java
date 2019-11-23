package io.ventura.nexmark.beans;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.shaded.netty4.io.netty.util.Recycler;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public interface NexmarkEvent extends Serializable {

	int getEventType();

	long getEventId();

	public static final Recycler<AuctionEvent> AUCTIONS_RECYCLER = new Recycler<AuctionEvent>(2 * 1024 * 1024) {
		@Override
		protected AuctionEvent newObject(Handle handle) {
			return new AuctionEvent(handle);
		}
	};

	public static final Recycler<BidEvent> BIDS_RECYCLER = new Recycler<BidEvent>(2 * 1024 * 1024) {
		@Override
		protected BidEvent newObject(Handle handle) {
			return new BidEvent(handle);
		}
	};

	void writeKey(ByteBuffer b);

	void serialize(DataOutputSerializer out) throws IOException;

	class AuctionEvent extends AuctionEvent0 implements NexmarkEvent {
		public AuctionEvent(Recycler.Handle handle) {
			super(handle);
		}

		@Override
		public int getEventType() {
			return 0;
		}

		@Override
		public long getEventId() {
			return auctionId;
		}

		@Override
		public void writeKey(ByteBuffer b) {
			b.putLong(auctionId);
		}

		@Override
		public void serialize(DataOutputSerializer output) throws IOException {
			output.write(0);
			output.writeLong(timestamp);
			output.writeLong(auctionId);
			output.writeLong(itemId);
			output.writeUTF(name);
			output.writeUTF(descr);
			output.writeLong(personId);
			output.writeDouble(initialPrice);
			output.writeLong(start);
			output.writeLong(end);
			output.writeLong(categoryId);
			output.writeLong(ingestionTimestamp);
		}

		public NexmarkEvent initEx(long auctionId, long matchingPerson, long timestamp, long end, ThreadLocalRandom r) {
			super.init(auctionId, matchingPerson, timestamp, end, r);
			return this;
		}

		public AuctionEvent initEx(long timestamp, long auctionId, String name, String descr, long itemId, long personId, Double initialPrice, long categoryId, long start, long end, long ingestionTimestamp) {
			super.init(timestamp, auctionId, name, descr, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
			return this;
		}
	}

	class BidEvent extends BidEvent0 implements NexmarkEvent {
		public BidEvent(Recycler.Handle handle) {
			super(handle);
		}

		@Override
		public int getEventType() {
			return 1;
		}

		@Override
		public long getEventId() {
			return auctionId;
		}

		@Override
		public void writeKey(ByteBuffer b) {
			b.putLong(auctionId);
		}

		@Override
		public void serialize(DataOutputSerializer output) throws IOException {
			output.write(1);
			output.writeLong(ingestionTimestamp);
			output.writeLong(timestamp);
			output.writeLong(auctionId);
			output.writeLong(personId);
			output.writeLong(bidId);
			output.writeDouble(bid);
		}


		public NexmarkEvent initEx(long ingestionTimestamp, long timestamp, long auctionId, long personId, long bidId, double bid) {
			super.init(ingestionTimestamp, timestamp, auctionId, personId, bidId, bid);
			return this;
		}
	}

	class PersonEvent extends NewPersonEvent0 implements NexmarkEvent {

		public PersonEvent(long personId, long timestamp, ThreadLocalRandom r) {
			super(personId, timestamp, r);
		}

		public PersonEvent(long timestamp, long personId, String name, String email, String city, String country, String province, String zipcode, String homepage, String creditcard, long ingestionTimestamp) {
			super(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, ingestionTimestamp);
		}

		@Override
		public int getEventType() {
			return 2;
		}

		@Override
		public long getEventId() {
			return personId;
		}

		@Override
		public void writeKey(ByteBuffer b) {
			b.putLong(personId);
		}

		@Override
		public void serialize(DataOutputSerializer output) throws IOException {
			output.write(2);
			output.writeLong(timestamp);
			output.writeLong(personId);
			output.writeUTF(name);
			output.writeUTF(email);
			output.writeUTF(city);
			output.writeUTF(country);
			output.writeUTF(province);
			output.writeUTF(zipcode);
			output.writeUTF(homepage);
			output.writeUTF(creditcard);
			output.writeLong(ingestionTimestamp);
		}
	}

}
