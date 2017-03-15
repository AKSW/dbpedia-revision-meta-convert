import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.memory.MemoryStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * This class can be used to merge the output of the DBpedia Revison extraction with
 * a DBpedia dataset
 * 
 * @TODO Improve handling of input XML data --> Use X-Path or SAX-Parser!
 * @author kay
 *
 */
public class Main {
	
	static Pattern dateErrorPattern = Pattern.compile("(?<=\")([0-9]{1,2}\\/[0-9]{4})(?=\"\\^\\^xsd\\:gYearMonth)");
	static Pattern splitPatternSlash = Pattern.compile("/");
	static Pattern splitPatternHyphen = Pattern.compile("-");


	
	public static RepositoryConnection getConnection(final String sparqlEndpoint) throws RepositoryException {
		String sparqlEndpointFinal;
		if (null == sparqlEndpoint) {
			sparqlEndpointFinal = "http://vmdbpedia.informatik.uni-leipzig.de:8890/sparql";
		} else {
			sparqlEndpointFinal = sparqlEndpoint;
		}
		Repository repo = new SPARQLRepository(sparqlEndpointFinal);
		repo.initialize();
		
		RepositoryConnection connection = repo.getConnection();
		return connection;
	}
	
	public static String createUriQuery(final String uri, final int limit, final int offset) {
		String queryString = "SELECT ?p ?o WHERE { <" + uri + "> ?p ?o } LIMIT " + Integer.toString(limit) + " OFFSET " + Integer.toString(offset);
		return queryString;
	}
	
	public static String createMainMetadataQuery() {
		String queryString = "SELECT * WHERE {?s a <http://www.w3.org/ns/prov#Revision> . "
				+ "?s <http://purl.org/dc/element/1.1/subject> ?dbpediaUri . "
				+ "OPTIONAL { ?s <http://purl.org/dc/element/1.1/created> ?created . } "
				+ "OPTIONAL { ?s <http://purl.org/dc/element/1.1/modified> ?modified . } "
				+ "OPTIONAL { ?s <http://ns.inria.fr/dbpediafr/voc#uniqueContributorNb> ?contributorCount . } "
				+ "OPTIONAL { ?s <http://www.w3.org/2004/03/trix/swp-2/isVersion> ?isVersion . } }";
		return queryString;
	}
	
	public static String createRevisionQuery() {
		String queryString = "SELECT * WHERE {?s a <http://www.w3.org/ns/prov#Revision> . }";
		return queryString;
	}
	
	public static TupleQueryResult executeQuery(final RepositoryConnection connection, final String queryString) throws QueryEvaluationException, RepositoryException, MalformedQueryException {
		TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		
		TupleQueryResult result = tupleQuery.evaluate();
		return result;
	}
	
	public static List<Statement> getStatements(final RepositoryConnection connection, final String uriString) throws RepositoryException {
		URI uri = new URIImpl(uriString);
/// TODO km: Check for solution: https://github.com/openlink/virtuoso-opensource/issues/214
//		if (false == connection.hasStatement(uri, null, null, false)) {
//			return null;
//		}
		
		RepositoryResult<Statement> results = null;
		try {
		 results = connection.getStatements(uri, null, null, false);
		
		
		 List<Statement> statements = null;
		 while (results.hasNext()) {
			 Statement statement = results.next();
			 if (null == statements) {
				 // create lazy
				 statements = new ArrayList<>();
			 }
			 
			 statements.add(statement);
		 }		 
		 results.close();

		 return statements;
		} catch (Exception e) {
			/// ignore for now --> check why input data is dirty!!
		} finally {
			if (null != results) {
				results.close();
			}
		}
		
		return null;
	}
	
	public static JsonObject createMetaFacts(final String key, final String value) {
		JsonObject created = new JsonObject();
		created.addProperty("type", "kv-rdf-meta");
		created.addProperty("key", key);
		created.addProperty("value", value);
		
		return created;
	}
	
	public static String createUriFromString(final String uriString) {
		return "<" + uriString + ">";
	}
	
	public static void writeResults(final BufferedWriter jsonMetaFile, final BufferedWriter rdfMetaFile, final String subjectUriString, final List<Statement> statements, final String prefixes, final String entity) throws RDFParseException, RDFHandlerException, IOException, RepositoryException, QueryEvaluationException, MalformedQueryException {
		
		// write out entity information in turtle
		rdfMetaFile.append(entity).append("\n");
		rdfMetaFile.flush();
		
		// create valid turtle which includes prefixes
		StringBuilder entityInfo = new StringBuilder();
		entityInfo.append(prefixes);
		entityInfo.append(entity);

		// create JSON object
		JsonObject mainJsonObject = new JsonObject();
		JsonArray statementGroups = new JsonArray();
		mainJsonObject.add("statementgroups", statementGroups);
		
		URI subjectUri = new URIImpl(subjectUriString);
		
		JsonObject statementsObject = new JsonObject();
		statementGroups.add(statementsObject);
		
		statementsObject.addProperty("groupid", "<" + subjectUri + "-Statements>");
	
		if (true) {
			JsonArray statementArray = new JsonArray();
			statementsObject.add("statements", statementArray);
			
			for (Statement statement : statements) {
				
				OutputStream outputStream = new ByteArrayOutputStream();
				RDFWriter rdfWriter = Rio.createWriter(RDFFormat.NTRIPLES, outputStream);
				Rio.write(statement, rdfWriter);
				
				JsonObject statementObject = new JsonObject();
				statementObject.addProperty("type", "triple");
//				statementObject.addProperty("tuple", "<" + subjectUri.stringValue() + "> <" + predicate.stringValue() + "> " + objectString + " .");
				statementObject.addProperty("tuple", outputStream.toString());
				statementObject.addProperty("sid", "");
				
				outputStream.close();
				
				statementArray.add(statementObject);
			}
		}
		
		JsonArray mids = new JsonArray();
		statementsObject.add("mids", mids);
		
		String metaUri = subjectUri + "-Meta";
		String revisionUri = subjectUri + "-Revisions";
		mids.add(Main.createUriFromString(metaUri));
		mids.add(Main.createUriFromString(revisionUri));
		
		Repository repository = new SailRepository(new MemoryStore());
		repository.initialize();
		
		RepositoryConnection connection = repository.getConnection();
		
		// add entity information
		InputStream entityStream = new ByteArrayInputStream(entityInfo.toString().getBytes());
		connection.add(entityStream, "http://test.org", RDFFormat.TURTLE);		
		entityStream.close();
		
		JsonArray metadataArray = new JsonArray();
		mainJsonObject.add("metadata", metadataArray);
		
		JsonObject metadata = new JsonObject();
		metadataArray.add(metadata);
		
		metadata.addProperty("groupid", Main.createUriFromString(metaUri));
		
		JsonArray metadataFacts_Meta = new JsonArray();
		metadata.add("metadataFacts", metadataFacts_Meta);
		
		Value mainRevisionUri = null;
		String queryString = Main.createMainMetadataQuery();
		TupleQueryResult results = Main.executeQuery(connection, queryString);
		while (results.hasNext()) {
			BindingSet bindingSet = results.next();
			
			mainRevisionUri = bindingSet.getValue("s");
			{
				Literal createdValue = (Literal) bindingSet.getValue("created");
				if (null != createdValue) {
					JsonObject created = Main.createMetaFacts("<http://purl.org/dc/element/1.1/created>", "\"" + createdValue.stringValue() + "\"^^<" + createdValue.getDatatype().stringValue() + ">");
					metadataFacts_Meta.add(created);
				}
			}
			
			{
				Literal modifiedValue = (Literal) bindingSet.getValue("modified");
				if (null != modifiedValue) {
					JsonObject modified = Main.createMetaFacts("<http://purl.org/dc/element/1.1/modified>", "\"" + modifiedValue.stringValue() + "\"^^<" + modifiedValue.getDatatype().stringValue() + ">");
					metadataFacts_Meta.add(modified);
				}
			}
			
			{
				Literal contributorCount = (Literal) bindingSet.getValue("contributorCount");
				if (null != contributorCount) {
					JsonObject contributorCountJson = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#uniqueContributorNb>", "\"" + contributorCount.stringValue() + "\"^^<" + contributorCount.getDatatype().stringValue() + ">");
					metadataFacts_Meta.add(contributorCountJson);
				}
			}
			
			{
				Literal isVersion = (Literal) bindingSet.getValue("isVersion");
				if (null != isVersion) {
					JsonObject isVersionJson = Main.createMetaFacts("<http://www.w3.org/2004/03/trix/swp-2/isVersion>", "\"" + isVersion.stringValue() + "\"^^<" + isVersion.getDatatype().stringValue() + ">");
					metadataFacts_Meta.add(isVersionJson);
				}
			}
		}
	
		Map<String, String> yearValueMap = new TreeMap<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				
				return o2.compareTo(o1);
			}
		});
		
		String yearQuery = "SELECT ?year ?valueYear WHERE { ?s <http://ns.inria.fr/dbpediafr/voc#revPerYear> ?bnYear ."
						+ " ?bnYear <http://purl.org/dc/element/1.1/date> ?year ."
						+ " ?bnYear <" + RDF.VALUE.stringValue() + "> ?valueYear . }  LIMIT 10000 ";
		TupleQueryResult yearResult = Main.executeQuery(connection, yearQuery);
		while (yearResult.hasNext()) {
			BindingSet resultBinding = yearResult.next();
			
			Value yearValue = resultBinding.getValue("year");
			Value value = resultBinding.getValue("valueYear");
			
			if (null == yearValue || null == value) {
				continue;
			}
			
			yearValueMap.put(yearValue.stringValue(), value.stringValue());
		}
		
		Map<String, String> monthValueMap = new TreeMap<>(new Comparator<String>() {
			
			
			@Override
			public int compare(String o1, String o2) {
				String[] dateParts1 = splitPatternHyphen.split(o1);
				String[] dateParts2 = splitPatternHyphen.split(o2);
				
				// compare years
				int yearsDiff = dateParts2[0].compareTo(dateParts1[0]);
				if (0 != yearsDiff) {
					return yearsDiff;
				}
				
				// compare months
				return dateParts2[1].compareTo(dateParts1[1]);
			}
		});
		
		String monthQuery = "SELECT ?date ?valueDate WHERE { ?s <http://ns.inria.fr/dbpediafr/voc#revPerMonth> ?bnDate ."
						+ " ?bnDate <http://purl.org/dc/element/1.1/date> ?date ."
						+ " ?bnDate <" + RDF.VALUE.stringValue() + "> ?valueDate . } LIMIT 10000";
		TupleQueryResult monthResult = Main.executeQuery(connection, monthQuery);
		while (monthResult.hasNext()) {
			BindingSet resultBinding = monthResult.next();
			
			Value dateValue = resultBinding.getValue("date");
			Value value = resultBinding.getValue("valueDate");
			
			if (null == dateValue || null == value) {
				continue;
			}
			
			monthValueMap.put(dateValue.stringValue(), value.stringValue());
		}
		
		final int maxDateCount = 2;
		int dateCount = 1;
		for (String date : monthValueMap.keySet()) {
//			System.out.println("Year: " + date + " value: " + monthValueMap.get(date));			
			
			JsonObject monthFact = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#revPerLastMonth" + dateCount++ + ">",
										"\"" + monthValueMap.get(date) + "\"^^<" + "http://www.w3.org/2001/XMLSchema#integer" + ">");
			metadataFacts_Meta.add(monthFact);			
			
			if (dateCount > maxDateCount) {
				break;
			}
		}
		
		dateCount = 1;
		for (String year : yearValueMap.keySet()) {			
			String value;
			if (1 == dateCount && year.equals("2016")) {
				value =  yearValueMap.get(year);
			} else if (2 == dateCount && year.equals("2015")) {
				value =  yearValueMap.get(year);
			} else {
				value = "0";
			}
			
			JsonObject yearFact = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#revPerYear" + year + ">",
					"\"" + value + "\"^^<" + "http://www.w3.org/2001/XMLSchema#integer" + ">");
			metadataFacts_Meta.add(yearFact);
			
			if (++dateCount > maxDateCount) {
				break;
			}
		}		
		
		metadata.addProperty("grouptype", "flat");
		metadata.addProperty("hasMeta", Main.createUriFromString(metaUri + "-Meta"));
		
		JsonObject revision = new JsonObject();
		metadataArray.add(revision);

		revision.addProperty("groupid", Main.createUriFromString(revisionUri));
		
		JsonArray metadataFacts_Revisions = new JsonArray();
		revision.add("metadataFacts", metadataFacts_Revisions);
		
		JsonObject mainRevision = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#hasMainRevision>",
									Main.createUriFromString(mainRevisionUri.stringValue()));
		metadataFacts_Revisions.add(mainRevision);
		
		String revisionQuery = Main.createRevisionQuery();
		TupleQueryResult revisionResults = Main.executeQuery(connection, revisionQuery);
		while (revisionResults.hasNext()) {
			BindingSet binding = revisionResults.next();
			
			Value uri = binding.getValue("s");
			if (null == uri || uri.stringValue().equals(mainRevisionUri.stringValue())) {
				continue;
			}
			
			JsonObject revisionValue = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#hasOldRevision>", "<" + uri + ">");
			metadataFacts_Revisions.add(revisionValue);
		}
		
		revision.addProperty("grouptype", "flat");
		revision.addProperty("hasMeta", "");
		
		JsonObject metaMetadata = new JsonObject();
		metadataArray.add(metaMetadata);
		
		metaMetadata.addProperty("groupid", Main.createUriFromString(metaUri + "-Meta"));

		JsonArray metadataFacts_MetaMeta = new JsonArray();
		
		metaMetadata.addProperty("grouptype", "flat");
		metaMetadata.addProperty("hasMeta", "");
		
		metaMetadata.add("metadataFacts", metadataFacts_MetaMeta);
		
		String metametaUri = "http://ns.inria.fr/dbpediafr/resource#DBpediaGermanDataset_16_11_14";
		JsonObject metaMetaObject = Main.createMetaFacts("<http://ns.inria.fr/dbpediafr/voc#hasSource>",
				Main.createUriFromString(metametaUri));
		
		metadataFacts_MetaMeta.add(metaMetaObject);
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	    gson.toJson(mainJsonObject, jsonMetaFile);
	    jsonMetaFile.append("\n");
	    jsonMetaFile.flush();
	    
	    connection.close();
	}

	public static void main(String[] args) throws IOException, RDFParseException, RDFHandlerException, RepositoryException, MalformedQueryException, QueryEvaluationException {
		System.out.println("Hello World");

		long count = 0;
		
		String sparqlEndpoint = "http://vmdbpedia.informatik.uni-leipzig.de:8890/sparql";
		Repository repo = new SPARQLRepository(sparqlEndpoint);
		repo.initialize();
		
		System.out.println("Read in the following file: " + args[0]);
		System.out.println("Meta JSON File: " + args[1]);
		System.out.println("Meta RDF File: " + args[2]);
		System.out.println("SPARQL Endpoint: " + args[3]);
		
		File historicFile = new File(args[0]);
		if (false == historicFile.exists()) {
			System.err.println("File does not exist: " + historicFile);
			return;
		}
		
		RepositoryConnection connection = Main.getConnection(args[3]);	
		
		File jsonFile = new File(args[1]);
		BufferedWriter jsonWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jsonFile), "UTF-8"));
		BufferedWriter rdfWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(args[2])), "UTF-8"));
		
		Pattern newlinePattern = Pattern.compile("\\n");
		Pattern bugPattern = Pattern.compile("\\^\\^xsd\\:string[\\s\\t]+prov\\:");
		Pattern endEntityPattern = Pattern.compile("[\">\\]][\\^a-zA-Z:0-9]*[\\t\\s]\\.$");
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(historicFile));
		try {
			
			StringBuilder prefixes = new StringBuilder();
			
			String line = null;
			while (null != (line = bufferedReader.readLine())) {
				if (line.isEmpty()) {
					continue;
				}
				
				if (false == line.startsWith("@prefix")) {
					break; // reached end
				}
				
				prefixes.append(line).append("\n");
			}
			
			// write them out to RDF turtle file
			rdfWriter.append(prefixes).append("\n");
			
			boolean isFirst = true;
			long matchCount = 0;
			
			long startTime = System.currentTimeMillis();
			long startBlock = startTime;
			long blockMatchCount = 0;
			while (null != line) {
				StringBuilder matchedEntities = new StringBuilder();
				
				String wikipediaArticleName = null;
				
				if (isFirst) {					
					int start = line.indexOf("wiki/");
					int end = line.lastIndexOf(">");
					
					wikipediaArticleName = "=" + line.substring(start + "wiki/".length(), end) + "&";
				}
				
				boolean noKnownUri = false;
				String subjectUri = null;				
				List<Statement> matchingStatements = null;
				do {
					StringBuilder thisEntity = new StringBuilder();					
					if (isFirst) {
						thisEntity.append("\n").append(line).append("\n");
						
						
						isFirst = false;
					}
					
					// read entity
					boolean foundFinalMark = false;
					while (null != (line = bufferedReader.readLine())) {
						if (foundFinalMark && line.isEmpty()) {
							// found entity --> add it to output
							matchedEntities.append("\n").append(thisEntity).append("\n");
							break; // end of entity
						}
						
						// reached end of entity
						if (endEntityPattern.matcher(line).find()) {
							foundFinalMark = true;
						}
						
						// no further processing required
						if (noKnownUri) {
							continue;
						}
						

						if (null == subjectUri && line.contains("dc:subject")) {
							line = line.trim();
							int start = line.indexOf("<");
							int end = line.lastIndexOf(">");
							subjectUri = line.substring(start + 1, end);
							
//							subjectUri = "http://de.dbpedia.org/resource/Mark_Hurd";
							matchingStatements = Main.getStatements(connection, subjectUri);
							noKnownUri = null == matchingStatements || matchingStatements.isEmpty();
						}
						
						// just in case, we get an accidental new line!
						line = newlinePattern.matcher(line).replaceAll(" ");

						if (bugPattern.matcher(line).find()) {
							// handles bug in output framework							
							int endFirstLine = line.indexOf("^^xsd:string");
							String firstLine = line.substring(0, endFirstLine + "^^xsd:string".length());
							
							thisEntity.append(firstLine).append(" ;\n");
							
							String secondLine = line.substring(endFirstLine + "^^xsd:string".length() + 1);
							line = secondLine;
							
							thisEntity.append("\t").append(secondLine.trim()).append("\n");
						} else if (line.contains("\"NaN\"^^xsd") || line.contains("\"undefined\"^^xsd")) {
							continue; // ignore --> next line
						} else if (line.contains("^^xsd:gYearMonth")) {
							
							Matcher matcher = dateErrorPattern.matcher(line);
							if (!matcher.find()) {
								System.err.println("Did not find match in line: " + line);
							}
							
							String[] incorrectDate = splitPatternSlash.split(matcher.group(0));							
							line =  matcher.replaceAll(incorrectDate[1] + "-" + incorrectDate[0]);
							
							thisEntity.append(line).append("\n");
						} else {
							thisEntity.append(line).append("\n");
						}
					}
	
					if (false) {
						try {
						System.out.println("Entity: \n" + thisEntity);
						String tmp = thisEntity.toString();
						
						InputStream entityStream = new ByteArrayInputStream(thisEntity.toString().getBytes());
					    RDFParser rdfParser = new TurtleParser();
					    
					    List<Statement> statements = new ArrayList<>();
					    StatementCollector collector = new StatementCollector(statements);
					    rdfParser.setRDFHandler(collector);
					    
					    rdfParser.parse(entityStream, "http://test.org");
					    
					    entityStream.close();
						
						tmp = tmp.replaceAll("p3EE05929", "3EE05929");
						tmp = tmp.replaceAll("dip\\.", "di.");
						
						//thisEntity.replace(start, end, str)
						} catch (Exception e) {
							
							System.out.println("Error Entity: " + thisEntity);
							throw e;
						}
					}
					
					// get to the beginning of new entity
					while (null != line && false == line.startsWith("<") && null != (line = bufferedReader.readLine())) {
						
					}					

					// should store line of the new entity
					isFirst = true;
					
				} while (null != line && line.contains(wikipediaArticleName));
	
				if (null != matchingStatements && false == matchingStatements.isEmpty()) {
					++matchCount;
					++blockMatchCount;
					Main.writeResults(jsonWriter, rdfWriter, subjectUri, matchingStatements, prefixes.toString(), matchedEntities.toString());
				}		
				
				if (++count % 100 == 0) {
					long now = System.currentTimeMillis();
					long diffBlock = (now - startBlock) / 1000 + 1;
					long diffAverage = (now - startTime) / 1000 + 1;
					startBlock = now; // get current time for next round
					System.out.println("Checked entities: " + count +
										" which is: " + (100 / diffBlock) + " entities/second" +
										" which is on average: " + (count / diffAverage) + " entities/second" +
										" and matched: " + blockMatchCount + "(" + matchCount + ")");
					blockMatchCount = 0;
				}
			}
			
		} finally {
			bufferedReader.close();
			connection.close();
			jsonWriter.close();
			rdfWriter.close();
		}
		
		System.out.println("Finished!");
	}

}
