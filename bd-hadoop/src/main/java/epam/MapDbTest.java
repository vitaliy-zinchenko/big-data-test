package epam;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public class MapDbTest {

    public static class MapComparator implements Comparator<String>, Serializable {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    public static void main(String[] args) {
        DB db = DBMaker.newMemoryDB().make();
        db.getCatalog().put("map.comparator", new MapComparator());

        ConcurrentNavigableMap treeMap = db.getTreeMap("map");

        treeMap.put("qzz","4");
        treeMap.put("qa", "12");
        treeMap.put("qz", "1");

        for(Object e : treeMap.entrySet()) {
            System.out.println(e);
        }

        db.commit();
        db.close();
    }

}
