package io.ventura.nexmark.common;

import io.ventura.nexmark.beans.NewPersonEvent0;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class PersonsFlatMapper  implements FlatMapFunction<NewPersonEvent0[], NewPersonEvent0> {
	@Override
	public void flatMap(NewPersonEvent0[] items, Collector<NewPersonEvent0> out) throws Exception {
		for (int i = 0; i < items.length; i++) {
			out.collect(items[i]);
		}
	}
}