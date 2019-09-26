package io.ventura.nexmark.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

public class JoinHelper {

	public static class Input1Tagger<T1, T2> implements MapFunction<T1, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T1 value) throws Exception {
			return TaggedUnion.one(value);
		}
	}

	public static class Input2Tagger<T1, T2> implements MapFunction<T2, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T2 value) throws Exception {
			return TaggedUnion.two(value);
		}
	}

	public static class UnionKeySelector<T1, T2, KEY> implements KeySelector<TaggedUnion<T1, T2>, KEY> {
		private static final long serialVersionUID = 1L;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;

		public UnionKeySelector(KeySelector<T1, KEY> keySelector1,
				KeySelector<T2, KEY> keySelector2) {
			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;
		}

		@Override
		public KEY getKey(TaggedUnion<T1, T2> value) throws Exception{
			if (value.isOne()) {
				return keySelector1.getKey(value.getOne());
			} else {
				return keySelector2.getKey(value.getTwo());
			}
		}
	}

	public static class TaggedUnion<T1, T2> {
		private final T1 one;
		private final T2 two;

		private TaggedUnion(T1 one, T2 two) {
			this.one = one;
			this.two = two;
		}

		public boolean isOne() {
			return one != null;
		}

		public boolean isTwo() {
			return two != null;
		}

		public T1 getOne() {
			return one;
		}

		public T2 getTwo() {
			return two;
		}

		public static <T1, T2> TaggedUnion<T1, T2> one(T1 one) {
			return new TaggedUnion<>(one, null);
		}

		public static <T1, T2> TaggedUnion<T1, T2> two(T2 two) {
			return new TaggedUnion<>(null, two);
		}
	}

	public static class UnionTypeInfo<T1, T2> extends TypeInformation<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeInformation<T1> oneType;
		private final TypeInformation<T2> twoType;

		public UnionTypeInfo(TypeInformation<T1> oneType,
				TypeInformation<T2> twoType) {
			this.oneType = oneType;
			this.twoType = twoType;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 2;
		}

		@Override
		public int getTotalFields() {
			return 2;
		}

		@Override
		@SuppressWarnings("unchecked, rawtypes")
		public Class<TaggedUnion<T1, T2>> getTypeClass() {
			return (Class) TaggedUnion.class;
		}

		@Override
		public boolean isKeyType() {
			return true;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> createSerializer(ExecutionConfig config) {
			return new UnionSerializer<>(oneType.createSerializer(config), twoType.createSerializer(config));
		}

		@Override
		public String toString() {
			return "TaggedUnion<" + oneType + ", " + twoType + ">";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UnionTypeInfo) {
				@SuppressWarnings("unchecked")
				UnionTypeInfo<T1, T2> unionTypeInfo = (UnionTypeInfo<T1, T2>) obj;

				return unionTypeInfo.canEqual(this) && oneType.equals(unionTypeInfo.oneType) && twoType.equals(unionTypeInfo.twoType);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 *  oneType.hashCode() + twoType.hashCode();
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionTypeInfo;
		}
	}

	private static class UnionSerializer<T1, T2> extends TypeSerializer<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeSerializer<T1> oneSerializer;
		private final TypeSerializer<T2> twoSerializer;

		public UnionSerializer(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			this.oneSerializer = oneSerializer;
			this.twoSerializer = twoSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> duplicate() {
			return this;
		}

		@Override
		public TaggedUnion<T1, T2> createInstance() {
			return null;
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from, TaggedUnion<T1, T2> reuse) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(TaggedUnion<T1, T2> record, DataOutputView target) throws IOException {
			if (record.isOne()) {
				target.writeByte(1);
				oneSerializer.serialize(record.getOne(), target);
			} else {
				target.writeByte(2);
				twoSerializer.serialize(record.getTwo(), target);
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(TaggedUnion<T1, T2> reuse,
				DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			byte tag = source.readByte();
			target.writeByte(tag);
			if (tag == 1) {
				oneSerializer.copy(source, target);
			} else {
				twoSerializer.copy(source, target);
			}
		}

		@Override
		public int hashCode() {
			return 31 * oneSerializer.hashCode() + twoSerializer.hashCode();
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object obj) {
			if (obj instanceof UnionSerializer) {
				UnionSerializer<T1, T2> other = (UnionSerializer<T1, T2>) obj;

				return other.canEqual(this) && oneSerializer.equals(other.oneSerializer) && twoSerializer.equals(other.twoSerializer);
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionSerializer;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new UnionSerializerConfigSnapshot<>(oneSerializer, twoSerializer);
		}

		@Override
		public CompatibilityResult<TaggedUnion<T1, T2>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof UnionSerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
					((UnionSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T1> oneSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(0).f1,
					oneSerializer);

				CompatibilityResult<T2> twoSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(1).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(1).f1,
					twoSerializer);

				if (!oneSerializerCompatResult.isRequiresMigration() && !twoSerializerCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else if (oneSerializerCompatResult.getConvertDeserializer() != null && twoSerializerCompatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
						new UnionSerializer<>(
							new TypeDeserializerAdapter<>(oneSerializerCompatResult.getConvertDeserializer()),
							new TypeDeserializerAdapter<>(twoSerializerCompatResult.getConvertDeserializer())));
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} for the {@link UnionSerializer}.
	 */
	public static class UnionSerializerConfigSnapshot<T1, T2> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public UnionSerializerConfigSnapshot() {}

		public UnionSerializerConfigSnapshot(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			super(oneSerializer, twoSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
