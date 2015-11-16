package org.webdatacommons.webtables.extraction;

import java.io.IOException;
import java.util.List;

import org.jsoup.nodes.Document;

import org.webdatacommons.webtables.extraction.model.DocumentMetadata;
import org.webdatacommons.webtables.extraction.stats.StatsKeeper;
import org.webdatacommons.webtables.tools.data.Dataset;

public interface ExtractionAlgorithm {

	public abstract List<Dataset> extract(Document doc,
			DocumentMetadata metadata) throws IOException, InterruptedException;

	public abstract StatsKeeper getStatsKeeper();
	
}