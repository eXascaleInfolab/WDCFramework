package org.webdatacommons.structureddata.tablegen;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;

public class ParserTest {
	
	private static Logger log = Logger.getLogger(ParserTest.class);


	public static void main(String[] args) throws RDFParseException,
			RDFHandlerException, FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		RDFParser p = new RobustNquadsParser();
		//RDFParser p = new NQuadsParser();

		p.setStopAtFirstError(false);
		
		p.setStopAtFirstError(false);

		p.setRDFHandler(new RDFHandler() {
			
			@Override
			public void startRDF() throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				log.info(st);
			}
			
			@Override
			public void handleNamespace(String prefix, String uri)
					throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void handleComment(String comment) throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void endRDF() throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
		});
		p.parse(new FileReader(new File("/home/hannes/Desktop/foo/tailfail.nq")),
				"foo://");
	}

}
