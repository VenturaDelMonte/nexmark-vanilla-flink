package io.ventura.nexmark.common;

import io.ventura.nexmark.beans.AuctionEvent0;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class AuctionsFlatMapper implements FlatMapFunction<AuctionEvent0[], AuctionEvent0> {
	@Override
	public void flatMap(AuctionEvent0[] items, Collector<AuctionEvent0> out) throws Exception {
		for (int i = 0; i < items.length; i++) {
			out.collect(items[i]);
		}
	}
}