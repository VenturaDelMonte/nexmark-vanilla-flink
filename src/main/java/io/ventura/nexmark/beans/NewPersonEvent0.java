package io.ventura.nexmark.beans;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import javax.annotation.Nonnull;
import java.io.Serializable;

public class NewPersonEvent0 implements Serializable {


	public long timestamp;
	public long personId;
	public String name;
	public String email;
	public String city;
	public String country;
	public String province;
	public String zipcode;
	public String homepage;
	public String creditcard;
	public long ingestionTimestamp;

	public NewPersonEvent0() {
	}

	public NewPersonEvent0(long timestamp,
						  long personId,
						  String name,
						  String email,
						  String city,
						  String country,
						  String province,
						  String zipcode,
						  String homepage,
						  String creditcard) {
		this(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, System.currentTimeMillis());
	}

	public NewPersonEvent0(long timestamp,
						  long personId,
						  @Nonnull String name,
						  @Nonnull String email,
						  @Nonnull String city,
						  @Nonnull String country,
						  @Nonnull  String province,
						  @Nonnull String zipcode,
						  @Nonnull String homepage,
						  @Nonnull String creditcard,
						  long ingestionTimestamp) {
		this.timestamp = timestamp;
		this.personId = personId;
		this.email = email;
		this.creditcard = creditcard;
		this.city = city;
		this.name = name;
		this.country = country;
		this.province = province;
		this.zipcode = zipcode;
		this.homepage = homepage;
		this.ingestionTimestamp = ingestionTimestamp;
	}

	public String getName() {
		return name;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public long getPersonId() {
		return personId;
	}

	public String getEmail() {
		return email;
	}

	public String getCreditcard() {
		return creditcard;
	}

	public String getCity() {
		return city;
	}

	public String getCountry() {
		return country;
	}

	public String getProvince() {
		return province;
	}

	public String getZipcode() {
		return zipcode;
	}

	public String getHomepage() {
		return homepage;
	}

	public long getIngestionTimestamp() {
		return ingestionTimestamp;
	}

	public static class NewPersonEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<NewPersonEvent0> {

		public NewPersonEventKryoSerializer() {

		}

		@Override
		public void write(Kryo kryo, Output output, NewPersonEvent0 event) {
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
		public NewPersonEvent0 read(Kryo kryo, Input input, Class<NewPersonEvent0> aClass) {
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

			return new NewPersonEvent0(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, ingestionTimestamp);
		}
}
}