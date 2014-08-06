package org.webdatacommons.structureddata.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.extractor.ExtractionException;
import org.apache.log4j.Logger;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.FlexBuffer;
import org.junit.Test;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.extractor.RDFExtractor.ExtractorResult;

public class ExtractorTest {

	private static Logger log = Logger.getLogger(ExtractorTest.class);

	// TODO: add test files for html-mf-species, html-mf-license,
	// html-mf-hlisting, html-mf-hresume, html-mf-hreview, html-mf-hrecipe

	@Test
	public void guesserTest() throws IOException {
		File dir = new File(getClass().getClassLoader()
				.getResource("exampledata").getFile());

		String[] testfiles = dir.list();
		if (testfiles == null) {
			log.warn("Unable to find test files in directory " + dir);
			assertTrue(false);
		}

		for (int i = 0; i < testfiles.length; i++) {
			File testfile = new File(dir.toString() + File.separator
					+ testfiles[i]);
			String data = readFileAsString(testfile);

			Map<String, Boolean> matchingGuessers = new HashMap<String, Boolean>();
			for (Map.Entry<String, Pattern> guesser : RDFExtractor.dataGuessers
					.entrySet()) {
				Matcher m = guesser.getValue().matcher(data);
				if (m.find()) {
					matchingGuessers.put(guesser.getKey(), Boolean.TRUE);
				}
			}
			if (matchingGuessers.entrySet().size() == 0) {
				log.warn("File " + testfile
						+ " was not matched by any guesser.");
				File tempOutFile = File.createTempFile("tempoutput_", "nq");
				RDFExtractor ex = new RDFExtractor(new FileOutputStream(
						tempOutFile));

				ExtractorResult res = ex.extract(new ArcFileItem()
						.setContent(new FlexBuffer(data.getBytes(), false))
						.setArcFileName(testfile.toString())
						.setUri(testfile.toURI().toString())
						.setMimeType("text/html"));

				for (String extractor : RDFExtractor.EXTRACTORS) {
					if (Long.parseLong(res.getExtractorTriples().get(extractor)) > 0) {
						log.warn("However, extractor " + extractor + " found "
								+ res.getExtractorTriples().get(extractor)
								+ " triples.");
					}
				}
			}
			assertTrue(matchingGuessers.entrySet().size() > 0);
		}
	}

	private static String readFileAsString(File file)
			throws java.io.IOException {
		byte[] buffer = new byte[(int) file.length()];
		BufferedInputStream f = new BufferedInputStream(new FileInputStream(
				file));
		f.read(buffer);
		f.close();
		return new String(buffer);
	}

	@Test
	public void licenseExtractorTest() throws FileNotFoundException,
			IOException, ExtractionException {
		RDFExtractor e = new RDFExtractor(new FileOutputStream("/dev/null"));
		ArcFileItem i = new ArcFileItem();
		i.setContent(new FlexBuffer(readFileAsString(
				new File("src/test/resources/exampledata/license.webpage"))
				.getBytes(), false));

		ExtractorResult r = e.extract(i);
		assertTrue(r.getTotalTriples() > 0);
	}
}
