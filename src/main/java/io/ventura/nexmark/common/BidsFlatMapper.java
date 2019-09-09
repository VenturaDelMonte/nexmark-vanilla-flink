package io.ventura.nexmark.common;

import io.ventura.nexmark.beans.BidEvent0;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class BidsFlatMapper extends RichFlatMapFunction<BidEvent0[], BidEvent0> {
	@Override
	public void flatMap(BidEvent0[] buffer, Collector<BidEvent0> collector) throws Exception {
		for (int i = 0; i < buffer.length; i++) {
			collector.collect(buffer[i]);
		}
	}
}