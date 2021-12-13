package io.ventura.nexmark.cm;

import java.io.Serializable;

public class CmRecord implements Serializable {
	public long timestamp;
	public long jobId;
	public long taskId;
	public long machineId;
	public int eventType;
	public int userId;
	public int category;
	public int priority;
	public float cpu;
	public float ram;
	public float disk;
	public int constraints;

	public CmRecord() {

	}

	public CmRecord(String line) {
		String[] words = line.split("\\s+");
		if (words.length == 12) {
			timestamp = Long.parseLong(words[0]);
			jobId = Long.parseLong(words[1]);
			taskId = Long.parseLong(words[2]);
			machineId = Long.parseLong(words[3]);
			eventType = Integer.parseInt(words[4]);
			userId = Integer.parseInt(words[5]);
			category = Integer.parseInt(words[6]);
			priority = Integer.parseInt(words[7]);
			cpu = Float.parseFloat(words[8]);
			ram = Float.parseFloat(words[9]);
			disk = Float.parseFloat(words[10]);
			constraints = Integer.parseInt(words[11]);
		} else {
			return;

		}
	}
}
