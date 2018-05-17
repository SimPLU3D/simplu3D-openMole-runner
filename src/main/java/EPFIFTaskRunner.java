import java.io.File;

import org.apache.commons.lang3.StringUtils;

import fr.ign.cogit.simplu3d.experiments.iauidf.tool.ParcelAttributeTransfert;
import fr.ign.cogit.simplu3d.experiments.openmole.EPFIFTask;
import fr.ign.cogit.simplu3d.io.feature.AttribNames;

public class EPFIFTaskRunner {
  public static String run(File folder, String dirName, File folderOut, File parameterFile, long seed) {
    AttribNames.setATT_CODE_PARC("IDPAR");
    EPFIFTask.USE_DEMO_SAMPLER = false;
    EPFIFTask.INTERSECTION = true;
    EPFIFTask.FLOOR_HEIGHT = 3;
    EPFIFTask.MAX_PARCEL_AREA = 10000; // now obsolete
    EPFIFTask.PARCEL_NAME = "parcelle.shp";
    
	ParcelAttributeTransfert.att_libelle_zone = "LIBELLE_ZO";
	ParcelAttributeTransfert.att_art_10_m = "B1_HAUT_M";
	ParcelAttributeTransfert.att_art_10_m_2 = "B2_HAUT_M";

    // String[] folderSplit = folder.getAbsolutePath().split(File.separator);
    String imu = dirName;// folderSplit[folderSplit.length - 1];

    String result = "";
    	 
    try {
    	result = EPFIFTask.run(folder, dirName, folderOut, parameterFile, seed);
    } catch (Exception e) {
      result = "# " + imu + " #\n";
      result += "# "+ e.toString() + "\n";
//      for (StackTraceElement s : e.getStackTrace())
//        result += "# " + s.toString() + "\n";
      result += "# " + imu + " #\n";
      e.printStackTrace();
    }
    return result;
  }

  public static void main(String[] args) {
	ParcelAttributeTransfert.att_libelle_zone = "LIBELLE_ZO";
	ParcelAttributeTransfert.att_art_10_m = "B1_HAUT_M";
	ParcelAttributeTransfert.att_art_10_m_2 = "B2_HAUT_M";
    String numrep = "77054334"; // "77059077";
    numrep = "78606";
    String foldName = "/home/imran/.openmole/imran-OptiPlex-9010/webui/projects/dataBasicSimu/idf/";
    foldName = "/home/imran/testoss/dirs2/";
    File folder = new File(foldName + numrep + "/");
    File folderOut = new File("/home/imran/testoss/out/" + numrep + "/");
    File parameterFile = new File("/home/imran/testoss/EPFIF/parameters_iauidf.xml");
    parameterFile = new File("/home/imran/workspace/simplu3d-openMole-runner/src/main/resources/parameters_iauidf_good.xml");
    long seed = 42L;
    String res = "";
    res = run(folder, numrep, folderOut, parameterFile, seed);
    System.out.println("result : " + res);
  }
}
