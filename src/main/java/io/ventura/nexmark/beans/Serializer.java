package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Serializer {

	public static class BidEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<NexmarkEvent.BidEvent> {

		public BidEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, NexmarkEvent.BidEvent bidEvent0) {
			output.writeLong(bidEvent0.ingestionTimestamp);
			output.writeLong(bidEvent0.timestamp);
			output.writeLong(bidEvent0.auctionId);
			output.writeLong(bidEvent0.personId);
			output.writeLong(bidEvent0.bidId);
			output.writeDouble(bidEvent0.bid);

			bidEvent0.recycle();
		}

		@Override
		public NexmarkEvent.BidEvent read(Kryo kryo, Input input, Class<NexmarkEvent.BidEvent> aClass) {
			long ingestionTimestamp = input.readLong();
			long timestamp = input.readLong();
			long auctionId = input.readLong();
			long personId = input.readLong();
			long bidId = input.readLong();
			double bid = input.readDouble();
			return (NexmarkEvent.BidEvent) NexmarkEvent.BIDS_RECYCLER.get().initEx(ingestionTimestamp, timestamp, auctionId, personId, bidId, bid);
		}
	}

	public static class AuctionEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<NexmarkEvent.AuctionEvent> {

    	public AuctionEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, NexmarkEvent.AuctionEvent event) {
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

//			event.handle.recycle(event);

			NexmarkEvent.AUCTIONS_RECYCLER.recycle(event, event.handle);
		}

		@Override
		public NexmarkEvent.AuctionEvent read(Kryo kryo, Input input, Class<NexmarkEvent.AuctionEvent> aClass) {
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

			return NexmarkEvent.AUCTIONS_RECYCLER.get().initEx(timestamp, auctionId, name, descr, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
		}
	}

	public static class NewPersonEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<NexmarkEvent.PersonEvent> {

		public NewPersonEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, NexmarkEvent.PersonEvent event) {
			output.writeLong(event.timestamp);
			output.writeLong(event.personId);
			output.writeString(event.name);
			output.writeString(event.email);
			output.writeString(event.city);
			output.writeString(event.country);
			output.writeString(event.province);
			output.writeString(event.zipcode);
			output.writeString(event.homepage);
			output.writeString(event.creditcard);
			output.writeLong(event.ingestionTimestamp);
		}

		@Override
		public NexmarkEvent.PersonEvent read(Kryo kryo, Input input, Class<NexmarkEvent.PersonEvent> aClass) {
			long timestamp = input.readLong();
			long personId = input.readLong();
			String name = input.readString();
			String email = input.readString();
			String city = input.readString();
			String country = input.readString();
			String province = input.readString();
			String zipcode = input.readString();
			String homepage = input.readString();
			String creditcard = input.readString();
			long ingestionTimestamp = input.readLong();

			return new NexmarkEvent.PersonEvent(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, ingestionTimestamp);
		}
	}

	public static class KafkaSerializationSchema implements KeyedSerializationSchema<NexmarkEvent> {

		@Override
		public byte[] serializeKey(NexmarkEvent element) {
			byte[] raw = new byte[8];
			ByteBuffer b = ByteBuffer.wrap(raw);
			element.writeKey(b);
			return raw;
		}

		@Override
		public byte[] serializeValue(NexmarkEvent element) {
			DataOutputSerializer out = new DataOutputSerializer(512);
			try {
				element.serialize(out);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			if (element instanceof NexmarkEvent.AuctionEvent) {
				NexmarkEvent.AUCTIONS_RECYCLER.recycle((NexmarkEvent.AuctionEvent) element, ((NexmarkEvent.AuctionEvent) element).handle);
			} else if (element instanceof NexmarkEvent.BidEvent) {
				((NexmarkEvent.BidEvent) element).recycle();
			}
			return out.getCopyOfBuffer();
		}

		@Override
		public String getTargetTopic(NexmarkEvent element) {
			return "nexmark-events";
		}
	}

	public static class KafkaDeserializationSchema implements KeyedDeserializationSchema<NexmarkEvent> {


		@Override
		public NexmarkEvent deserialize(
				byte[] messageKey,
				byte[] message,
				String topic,
				int partition,
				long offset) throws IOException {

			ByteBuffer keyBuf = ByteBuffer.wrap(messageKey);
			ByteBuffer payload = ByteBuffer.wrap(message);
			DataInputDeserializer wrapper = new DataInputDeserializer(payload);
			long key = keyBuf.getLong();
			byte type = wrapper.readByte();
			switch (type) {
				case 0: {
					long timestamp = wrapper.readLong();
					long auctionId = wrapper.readLong();
					long itemId = wrapper.readLong();
					String name = wrapper.readUTF();
					String descr = wrapper.readUTF();
					long personId = wrapper.readLong();
					double initialPrice = wrapper.readDouble();
					long start = wrapper.readLong();
					long end = wrapper.readLong();
					long categoryId = wrapper.readLong();
					long ingestionTimestamp = wrapper.readLong();
					return NexmarkEvent.AUCTIONS_RECYCLER.get().initEx(timestamp, auctionId, name, descr, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
				}
				case 1: {
					long ingestionTimestamp = wrapper.readLong();
					long timestamp = wrapper.readLong();
					long auctionId = wrapper.readLong();
					long personId = wrapper.readLong();
					long bidId = wrapper.readLong();
					double bid = wrapper.readDouble();
					return NexmarkEvent.BIDS_RECYCLER.get().initEx(ingestionTimestamp, timestamp, auctionId, personId, bidId, bid);
				}
				case 2: {
					long timestamp = wrapper.readLong();
					long personId = wrapper.readLong();
					String name = wrapper.readUTF();
					String email = wrapper.readUTF();
					String city = wrapper.readUTF();
					String country = wrapper.readUTF();
					String province = wrapper.readUTF();
					String zipcode = wrapper.readUTF();
					String homepage = wrapper.readUTF();
					String creditcard = wrapper.readUTF();
					long ingestionTimestamp = wrapper.readLong();

					return new NexmarkEvent.PersonEvent(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, ingestionTimestamp);
				}
				default: {
					break;
				}
			}
			return null;
		}

		@Override
		public boolean isEndOfStream(NexmarkEvent nextElement) {
			return false;
		}

		@Override
		public TypeInformation<NexmarkEvent> getProducedType() {
			return TypeInformation.of(new TypeHint<NexmarkEvent>() {});
		}

	}

}
