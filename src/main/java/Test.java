import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.FlexBuffer;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.util.WARCRecordUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Created by alberto on 23/02/16.
 */
public class Test {
    private static Logger log = Logger.getLogger(Test.class);

    // FIXME remove this if you do not want to get the links
    static Pattern linkPattern = Pattern
            .compile(
                    "<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    // FIXME remove this if you do not want to get the feeds
    static Pattern feedPattern = Pattern
            .compile(
                    "(<link[^>]*(?:\\s(?:type=[\\\"']?(application\\/rss\\+xml|application\\/atom\\+xml|application\\/rss|application\\/atom|application\\/rdf\\+xml|application\\/rdf|text\\/rss\\+xml|text\\/atom\\+xml|text\\/rss|text\\/atom|text\\/rdf\\+xml|text\\/rdf|text\\/xml|application\\/xml)[\\\"']?|rel=[\\\"']?(?:alternate)[\\\"']?))[^>]*>)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // To store pages in json format
    static JsonFactory jsonFactory = new JsonFactory();

    public static String headerKeyValue(String[] headers, String key,
                                        String dflt) {
        String line = headerValue(headers, key, null);
        if (line == null)
            return dflt;
        String[] pces = line.split(":");
        if (pces.length != 2)
            return dflt;
        return pces[1].trim();
    }

    private static void storePage(String url, String docCont, BufferedWriter pageBW) throws IOException {
        StringWriter x = new StringWriter();
        JsonGenerator g = jsonFactory.createGenerator(x);
        g.writeStartObject();
        g.writeStringField("url", url);
        g.writeStringField("content", docCont);
        g.writeEndObject();
        g.close();
        pageBW.write(x.toString());
        pageBW.write("\n");
//		pageBW.flush();
    }//storePage


    // some Hannes-TM HTTP header parsing kludges, way faster than libs
    public static String headerValue(String[] headers, String key, String dflt) {
        for (String hdrLine : headers) {
            if (hdrLine.toLowerCase().trim().startsWith(key.toLowerCase())) {
                return hdrLine.trim();
            }
        }
        return dflt;
    }

    public static void process(ReadableByteChannel fileChannel,
                               String inputFileKey) throws Exception {

        try {

            // create a tmp file to write the output for the triples to
            File tempOutputFile = File.createTempFile("dpef-triple-extraction", ".nq.gz");
            tempOutputFile.deleteOnExit();

            OutputStream tempOutputStream = new GZIPOutputStream(new FileOutputStream(tempOutputFile));
            RDFExtractor extractor = new RDFExtractor(tempOutputStream);

            // create file and stream for URLs.
            File tempOutputUrlFile = File.createTempFile("dpef-url-extraction", ".nq.gz");
            tempOutputUrlFile.deleteOnExit();

            GZIPOutputStream urlOS = new GZIPOutputStream(new FileOutputStream(tempOutputUrlFile));
            BufferedWriter urlBW = new BufferedWriter(new OutputStreamWriter(urlOS, "UTF-8"));

            // create file and stream for anchor.
            File tempOutputAnchorFile = File.createTempFile("dpef-anchor-extraction", ".nq.gz");
            tempOutputAnchorFile.deleteOnExit();

            GZIPOutputStream anchorOS = new GZIPOutputStream(new FileOutputStream(tempOutputAnchorFile));
            BufferedWriter anchorBW = new BufferedWriter(new OutputStreamWriter(anchorOS, "UTF-8"));

            // create file and stream for feed.
            File tempOutputFeedFile = File.createTempFile("dpef-feed-extraction", ".nq.gz");
            tempOutputFeedFile.deleteOnExit();

            GZIPOutputStream feedOS = new GZIPOutputStream(new FileOutputStream(tempOutputFeedFile));
            BufferedWriter feedBW = new BufferedWriter(new OutputStreamWriter(feedOS, "UTF-8"));

            // to store pages containing anchors
            File tempOutputAnchorPagesFile = File.createTempFile("dpef-anchor-pages", ".nq.gz");
            tempOutputAnchorPagesFile.deleteOnExit();

            GZIPOutputStream pageOS = new GZIPOutputStream(new FileOutputStream(tempOutputAnchorPagesFile));
            BufferedWriter pageBW = new BufferedWriter(new OutputStreamWriter(pageOS, "UTF-8"));

            // set name for data output
            String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_")
                    + ".nq.gz";
            // set name for stat output
            String outputStatsKey = "stats/ex_"
                    + inputFileKey.replace("/", "_") + ".csv.gz";
            // set name for url output
            String outputUrlKey = "urls/ex_" + inputFileKey.replace("/", "_")
                    + ".csv.gz";
            // set name for anchor output
            String outputAnchorKey = "anchor/ex_"
                    + inputFileKey.replace("/", "_") + ".csv.gz";
            // set name for feed output
            String outputFeedKey = "feed/ex_"
                    + inputFileKey.replace("/", "_") + ".csv.gz";
            // set name for page output
            String outputAnchorPagesKey = "anchor_pages/ex_"
                    + inputFileKey.replace("/", "_") + ".json.gz";


            // get archive reader
            final ArchiveReader reader = ArchiveReaderFactory.get(inputFileKey,
                    Channels.newInputStream(fileChannel), true);

            log.info(Thread.currentThread().getName() + ": Extracting data from " + inputFileKey + " ...");

            // number of pages visited for extraction
            long pagesTotal = 0;
            // number of pages parsed based on supported mime-type
            long pagesParsed = 0;
            // number of pages which contain an error
            long pagesErrors = 0;
            // number of pages which are likely to include triples
            long pagesGuessedTriples = 0;
            // number of pages including at least one triple
            long pagesTriples = 0;
            // number of anchors included in the pages
            long anchorTotal = 0;
            // number of feeds included in the pages
            long feedTotal = 0;
            // current time of the system when starting process.
            long start = System.currentTimeMillis();

            // TODO LOW write regex detection errors into SDB
            BufferedWriter bwriter = null;

            Iterator<ArchiveRecord> readerIt = reader.iterator();

            // read all entries in the ARC file
            while (readerIt.hasNext()) {

                ArchiveRecord record = readerIt.next();
                ArchiveRecordHeader header = record.getHeader();
                ArcFileItem item = new ArcFileItem();
                URI uri;

                item.setArcFileName(inputFileKey);

                // WARC contains lots of stuff. We only want HTTP responses
                if (!header.getMimetype().equals(
                        "application/http; msgtype=response")) {
                    continue;
                }
                if (pagesTotal % 1000 == 0) {
                    log.info(Thread.currentThread().getName() + ": " + pagesTotal + " / " + pagesParsed + " / "
                            + pagesTriples + " / " + pagesErrors);
                }

                try {

                    uri = new URI(header.getUrl());
                    String host = uri.getHost();
                    // we only write if its valid
                    urlBW.write(uri.toString() + "\n");
                    if (host == null) {
                        continue;
                    }
                } catch (URISyntaxException e) {
                    log.error("Invalid URI!!!", e);
                    continue;
                }

                String headers[] = WARCRecordUtils.getHeaders(record, true)
                        .split("\n");
                if (headers.length < 1) {
                    pagesTotal++;
                    continue;
                }

                // only consider HTML responses
                String contentType = headerKeyValue(headers, "Content-Type",
                        "text/html");
                if (!contentType.contains("html")) {
                    pagesTotal++;
                    continue;
                }

                byte[] bytes = IOUtils.toByteArray(WARCRecordUtils
                        .getPayload(record));

                if (bytes.length > 0) {

                    item.setMimeType(contentType);
                    item.setContent(new FlexBuffer(bytes, true));
                    item.setUri(uri.toString());

                    if (extractor.supports(item.getMimeType())) {
                        // do extraction (woo ho)
                        pagesParsed++;

                        // FIXME we can remove this if we do not want the anchors to be written
                        String docCont = item.getContent().toString("UTF-8");
                        // we only write anchors from not wikipedia pages
                        if (!uri.toString().contains("wikipedia")) {
                            if (docCont.contains("wikipedia.")) {
                                // now go through all the links and check
                                // weather
                                // they
                                // are good or not
                                Matcher pageMatcher = linkPattern
                                        .matcher(docCont);
                                // ArrayList<String> links = new
                                // ArrayList<String>();
                                Boolean anchorFound = false;
                                while (pageMatcher.find()) {
                                    // if
                                    // (pageMatcher.group(1).contains("wikipedia."))
                                    // {
                                    anchorBW.write(uri.toURL()
                                            + "\t"
                                            + pageMatcher.group(6)
                                            .replace("\n", " ")
                                            .replace("\r", " ")
                                            .replace("\t", " ") + "\t"
                                            + pageMatcher.group(1)
                                            .replace("\n", " ")
                                            .replace("\r", " ")
                                            .replace("\t", " ") + "\n");
                                    anchorTotal++;
                                    // }
                                    anchorFound = true;
                                }//while
                                // TODO remember to write the file in the end
                                if (anchorFound) {
                                    storePage("" + uri.toURL(), docCont, pageBW);
                                }
                            }//if
                        }//if

                        Matcher feedMatcher = feedPattern.matcher(docCont);
                        while (feedMatcher.find()) {
                            String group1 = feedMatcher.group(1);
                            String group2 = feedMatcher.group(2);
                            if (group1 != null && group2 != null)
                                feedBW.write(
                                        uri.toURL() + "\t" +
                                                group1.replace("\n", " ")
                                                        .replace("\r", " ")
                                                        .replace("\t", " ") + "\t" +
                                                group2.replace("\n", " ")
                                                        .replace("\r", " ")
                                                        .replace("\t", " ") + "\n");
                            else if (group1 != null) {
                                feedBW.write(
                                        uri.toURL() + "\t" +
                                                group1.replace("\n", " ")
                                                        .replace("\r", " ")
                                                        .replace("\t", " ") + "\t \n");
                            } else  {
                                log.debug("FeedRegex: first group = '" + group1 + "' second group: '" + group2 + "'");
                            }
                            feedTotal++; //TODO: @Victor, in which case do you want to increment the counter?
                        }//while

                        RDFExtractor.ExtractorResult result = extractor.extract(item);

                        // if we had an error, increment error count
                        if (result.hadError()) {
                            pagesErrors++;
                            pagesTotal++;
                            continue;
                        }
                        // if we found no triples, continue
                        if (result.hadResults()) {
                            // collect some other statistics

                            if (result.getTotalTriples() > 0) {
                                pagesTriples++;
                            } else {
                                log.debug("Could not find any triple in file, although guesser found something.");
                            }

                            // write statistics about pages without errors but not necessary with triples
                            pagesGuessedTriples++;
                        }
                    }
                    pagesTotal++;
                }
            }//while has next

            // we close the stream
            urlOS.flush();
            urlBW.close();
            anchorOS.flush();
            anchorBW.close();
            feedOS.flush();
            feedBW.close();
            pageOS.flush();
            pageBW.close();

            // and the data stream
            tempOutputStream.close();

            /**
             * write extraction results to s3, if at least one included item was
             * guessed to include triples
             */

            if (pagesGuessedTriples > 0) {
                log.debug("pagesGuessedTriples > 0");
            }

            if (pagesTotal > 0) {
                log.debug("pagesTotal > 0");
            }

            if (anchorTotal > 0) {
                log.debug("anchorTotal > 0");
            }

            if (feedTotal > 0) {
                log.debug("feedTotal > 0");
            }

            double duration = (System.currentTimeMillis() - start) / 1000.0;
            double rate = (pagesTotal * 1.0) / duration;

            // create data file statistics and return

            log.info("Extracted data from " + inputFileKey + " - parsed "
                    + pagesParsed + " pages in " + duration + " seconds, "
                    + rate + " pages/sec");

            reader.close();
            return;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new Exception(e.fillInStackTrace());
        }
    }

    public static void main(String[] args) throws Exception {
        // put here the path to a common crawl segment
        String testfilename = "/Users/alberto/Documents/Projects/CommonCrawl/CC-MAIN-20151001222139-00005-ip-10-137-6-227.ec2.internal.warc.gz";
        FileInputStream is = new FileInputStream(testfilename);
        ReadableByteChannel rbc = Channels.newChannel(is);
        // put here n'importe quoi...
        String inputFileKey = "test_warc.warc.gz";
        process(rbc, inputFileKey);
    }//main
}
