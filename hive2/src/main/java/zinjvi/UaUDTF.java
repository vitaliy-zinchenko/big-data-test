package zinjvi;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UaUDTF extends GenericUDTF {

    private PrimitiveObjectInspector stringOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1) {
            throw new UDFArgumentException("takes only one argument");
        }

        if (objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("takes a string as a parameter");
        }

        // input inspectors
        stringOI = (PrimitiveObjectInspector) objectInspectors[0];

        // output inspectors -- an object with two fields!
        List<String> fieldNames = new ArrayList<String>(4);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
        fieldNames.add("type");
        fieldNames.add("family");
        fieldNames.add("name");
        fieldNames.add("device");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public ArrayList<Object[]> processInputRecord(String userAgentString){
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        // ignoring null or empty input
        if (userAgentString == null || userAgentString.isEmpty()) {
            return result;
        }

//        String s = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)";
        UserAgent userAgent = new UserAgent(userAgentString);

        Browser browser = userAgent.getBrowser();
        System.out.println("UA type: " + browser.getBrowserType().getName());
        System.out.println("UA family: " + browser.getGroup().name());

        OperatingSystem operatingSystem = userAgent.getOperatingSystem();
        System.out.println("OS name: " + operatingSystem.getName());
        System.out.println("Device: " + operatingSystem.getDeviceType().getName());

//        String[] tokens = name.split("\\s+");

//        if (tokens.length == 2){
//            result.add(new Object[] { tokens[0], tokens[1] });
//        }else if (tokens.length == 4 && tokens[1].equals("and")){
//            result.add(new Object[] { tokens[0], tokens[3] });
//            result.add(new Object[] { tokens[2], tokens[3] });
//        }

        return result;
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        final String userAgentString = stringOI.getPrimitiveJavaObject(objects[0]).toString();

        if (userAgentString == null || userAgentString.isEmpty()) {
            return;
        }

//        String s = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)";
        UserAgent userAgent = new UserAgent(userAgentString);

        Browser browser = userAgent.getBrowser();
        System.out.println("UA type: " + browser.getBrowserType().getName());
        System.out.println("UA family: " +browser.getGroup().name());

        OperatingSystem operatingSystem = userAgent.getOperatingSystem();
        System.out.println("OS name: " + operatingSystem.getName());
        System.out.println("Device: " + operatingSystem.getDeviceType().getName());

        forward(new Object[]{
                    browser.getBrowserType().getName(),
                    browser.getGroup().name(),
                    operatingSystem.getName(),
                    operatingSystem.getDeviceType().getName()
        });

//        ArrayList<Object[]> results = processInputRecord(name);
//
//        Iterator<Object[]> it = results.iterator();

//        while (it.hasNext()){
//            Object[] r = it.next();
//            forward(r);
//        }
    }

    @Override
    public void close() throws HiveException {

    }
}
