package io.ventura.nexmark.source;

import io.ventura.nexmark.beans.NewPersonEvent0;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.nio.ByteBuffer;

public class PersonDeserializationSchema implements KeyedDeserializationSchema<NewPersonEvent0[]> {

		public static final int PERSON_RECORD_SIZE = 206;

		private static final TypeInformation<NewPersonEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<NewPersonEvent0[]>() {});


		private boolean isPartitionConsumed = false;

		public PersonDeserializationSchema() {
			//this.bytesToRead = (bytesToRead / PERSON_RECORD_SIZE) * PERSON_RECORD_SIZE;
//			this.bytesReadSoFar = 0;
		}

		@Override
		public NewPersonEvent0[] deserialize(
				byte[] messageKey,
				byte[] buffer,
				String topic,
				int partition,
				long offset) {

			Preconditions.checkArgument(buffer.length == 8192);
			ByteBuffer wrapper = ByteBuffer.wrap(buffer);
			int checksum = wrapper.getInt();
			int itemsInThisBuffer = wrapper.getInt();
			long newBacklog = wrapper.getLong();

			Preconditions.checkArgument(((8192 - 16) / PERSON_RECORD_SIZE) >= itemsInThisBuffer);

			Preconditions.checkArgument(checksum == 0x30011991);

			NewPersonEvent0[] data = new NewPersonEvent0[itemsInThisBuffer];

			byte[] tmp = new byte[32];

			long ingestionTimestamp = System.currentTimeMillis();

			StringBuilder helper = new StringBuilder(32 + 32 + 32 + 2);

			for (int i = 0; i < data.length; i++) {
				long id = wrapper.getLong();
				wrapper.get(tmp);
				String name = new String(tmp);
				wrapper.get(tmp);
				String surname = new String(tmp);
				wrapper.get(tmp);
				String email = helper
						.append(name)
						.append(".")
						.append(surname)
						.append("@")
						.append(new String(tmp))
						.toString();
				//name + "." + surname + "@" + new String(Arrays.copyOf(tmp, tmp.length));
				wrapper.get(tmp);
				String city = new String(tmp);
				wrapper.get(tmp);
				String country = new String(tmp);
				long creditCard0 = wrapper.getLong();
				long creditCard1 = wrapper.getLong();
				int a = wrapper.getInt();
				int b = wrapper.getInt();
				int c = wrapper.getInt();
				short maleOrFemale = wrapper.getShort();
				long timestamp = wrapper.getLong(); // 128
//				Preconditions.checkArgument(timestamp > 0);
				helper.setLength(0);
				data[i] = new NewPersonEvent0(
						timestamp,
						id,
						helper.append(name).append(" ").append(surname).toString(),
						email,
						city,
						country,
						"" + (a - c),
						"" + (b - c),
						email,
						"" + (creditCard0 + creditCard1),
						ingestionTimestamp);
				helper.setLength(0);
			}

//			bytesReadSoFar += buffer.length;
//			Preconditions.checkArgument(newBacklog < lastBacklog, "newBacklog: %s oldBacklog: %s", newBacklog, lastBacklog);
//			lastBacklog = newBacklog;
			isPartitionConsumed = newBacklog <= itemsInThisBuffer;
			return data;
		}

		@Override
		public boolean isEndOfStream(NewPersonEvent0[] nextElement) {
			return isPartitionConsumed;
		}

		@Override
		public TypeInformation<NewPersonEvent0[]> getProducedType() {
			return FLINK_INTERNAL_TYPE;
		}
	}