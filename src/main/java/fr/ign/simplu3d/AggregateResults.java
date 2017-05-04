package fr.ign.cogit.simplu3d;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureReader;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class AggregateResults {

	public static void aggregateParcels(File inputDirectory, File inputCSV, File outputFile) throws Exception {
		Reader in = new FileReader(inputCSV);
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';').withIgnoreEmptyLines().parse(in);
		Map<String, List<String>> map = new HashMap<>();
		for (CSVRecord record : records) {
			if (record.size() == 4) {
				String dir = record.get(0).trim();
				String par = record.get(1).trim();
				String nbobj = record.get(2).trim();
				String floorarea = record.get(3).trim();
				map.put(par, Arrays.asList(nbobj, floorarea, dir));
			}
		}
		String specs = "the_geom:Polygon,id:String,imu:String,found:Boolean,processed:Boolean,nb_objects:Integer,floor_area:Double";
		Stream<Path> stream = Files.find(Paths.get(inputDirectory.toURI()), 5, (filePath, fileAttr) -> filePath.endsWith("parcelle.shp"));
		try {
			FileDataStoreFactorySpi factory = new ShapefileDataStoreFactory();
			DataStore dataStore = factory.createDataStore(outputFile.toURI().toURL());
			String featureTypeName = "Object";
			SimpleFeatureType featureType = DataUtilities.createType(featureTypeName, specs);
			dataStore.createSchema(featureType);
			Transaction transaction = new DefaultTransaction("create");
			String typeName = dataStore.getTypeNames()[0];
			SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
			SimpleFeatureType type = featureSource.getSchema();
			SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
			ListFeatureCollection collection = new ListFeatureCollection(featureType);
			featureStore.setTransaction(transaction);
			int i = 1;
			Iterator<Path> it = stream.iterator();
			while (it.hasNext()) {
				Path file = it.next();
				System.out.println(file);
				try {
					ShapefileDataStore store = new ShapefileDataStore(file.toUri().toURL());
					FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = store.getFeatureReader();
					while (featureReader.hasNext()) {
						SimpleFeature feature = featureReader.next();
						Object geom = feature.getDefaultGeometry();
						String idPar = feature.getAttribute("IDPAR").toString();
						String imu = feature.getAttribute("IMU").toString();
						List<String> list = map.get(idPar);
						boolean found = false;
						boolean processed = false;
						int nbobj = 0;
						double area = 0;
						if (list != null) {
							found = true;
							nbobj = Integer.parseInt(list.get(0));
							if (nbobj != -69) {
								processed = true;
								area = Double.parseDouble(list.get(1));
							}
						}
						Object[] values = new Object[] { geom, idPar, imu, found, processed, nbobj, area };
						SimpleFeature simpleFeature = SimpleFeatureBuilder.build(type, values, String.valueOf(i++));
						collection.add(simpleFeature);
					}
					featureReader.close();
					store.dispose();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				featureStore.addFeatures(collection);
				transaction.commit();
			} catch (Exception problem) {
				problem.printStackTrace();
				transaction.rollback();
			} finally {
				transaction.close();
				dataStore.dispose();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (SchemaException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("I'm finished with parcels");
	}

	public static void aggregateBuildings(File inputDirectory, File outputFile) throws Exception {
		String specsBuildings = "the_geom:Polygon,height:Double,area:Double,volume:Double";		
		Stream<Path> streamB = Files.find(Paths.get(inputDirectory.toURI()), 100, (filePath, fileAttr) -> filePath.toString().endsWith("shp"));
		try {
			FileDataStoreFactorySpi factory = new ShapefileDataStoreFactory();
			DataStore dataStoreBuildings = factory.createDataStore(outputFile.toURI().toURL());
			String featureTypeNameB = "Buildings";
			SimpleFeatureType featureTypeBuildings = DataUtilities.createType(featureTypeNameB, specsBuildings);
			dataStoreBuildings.createSchema(featureTypeBuildings);
			Transaction transactionB = new DefaultTransaction("create");
			String typeNameB = dataStoreBuildings.getTypeNames()[0];
			SimpleFeatureSource featureSourceB = dataStoreBuildings.getFeatureSource(typeNameB);
			SimpleFeatureType typeB = featureSourceB.getSchema();
			SimpleFeatureStore featureStoreB = (SimpleFeatureStore) featureSourceB;
			ListFeatureCollection collectionB = new ListFeatureCollection(featureTypeBuildings);
			featureStoreB.setTransaction(transactionB);
			int building = 1;
			Iterator<Path> it = streamB.iterator();
			while (it.hasNext()) {
				Path file = it.next();
				System.out.println(file);
				try {
					ShapefileDataStore store = new ShapefileDataStore(file.toUri().toURL());
					FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = store.getFeatureReader();
					while (featureReader.hasNext()) {
						SimpleFeature feature = featureReader.next();
						Object geom = feature.getDefaultGeometry();
						double height = Double.parseDouble(feature.getAttribute("Hauteur").toString());
						double area = Double.parseDouble(feature.getAttribute("Aire").toString());
						double volume = Double.parseDouble(feature.getAttribute("Volume").toString());
						Object[] values = new Object[] { geom, height, area, volume };
						SimpleFeature simpleFeature = SimpleFeatureBuilder.build(typeB, values, String.valueOf(building++));
						collectionB.add(simpleFeature);
					}
					featureReader.close();
					store.dispose();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				featureStoreB.addFeatures(collectionB);
				transactionB.commit();
			} catch (Exception problem) {
				problem.printStackTrace();
				transactionB.rollback();
			} finally {
				transactionB.close();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (SchemaException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("I'm finished with buildings");
	}

	public static void main(String[] args) throws Exception {
		String outputFile = "iauidf_77/parcels.shp";
		String inputDirectory = "iauidf_77/testdata";
		String inputCSV = "iauidf_77/results/ouput.csv";
		
		String outputBuildingFile = "iauidf_77/buildings.shp";
		String inputResultDir = "iauidf_77/results/.";

		aggregateParcels(new File(inputDirectory), new File(inputCSV), new File(outputFile));
		aggregateBuildings(new File(inputResultDir), new File(outputBuildingFile));
		
	}
}
