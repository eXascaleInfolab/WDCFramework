package org.webdatacommons.webtables.extraction.stats;

import java.util.HashMap;
import java.util.Map;

public class HashMapStatsData extends StatsKeeper {

	protected HashMap<String, Integer> counters = new HashMap<String, Integer>();

	@Override
	public void incCounter(Enum<?> counter) {
		if (counters.containsKey(counter.name())) {
			int co = (Integer) counters.get(counter.name()).intValue();
			counters.put(counter.name(), Integer.valueOf(++co));
		} else {
			counters.put(counter.name(), 1);
		}
	}

	@Override
	public void reportProgress() {
	}

	public Map<String, Integer> statsAsMap() {
		return counters;
	}

	public void addMap(HashMap<String, Integer> addmap) {
		for (Map.Entry<String, Integer> e : addmap.entrySet())
			if (this.counters.containsKey(e.getKey()))
				this.counters.put(e.getKey(),
						e.getValue() + this.counters.get(e.getKey()));
			else
				this.counters.put(e.getKey(), e.getValue());
	}
}