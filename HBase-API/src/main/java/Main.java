import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @office D-2307
 * @date 16/11/24
 **/
public class Main {
    private Connection connection;
    private Admin admin;
    private TableName table = TableName.valueOf("Users");
    private static final String[] NAMES = {"Juan", "Luis", "Carlos", "Ana", "María", "Pedro", "Sofía", "Carmen", "Javier", "Laura"};
    private static final String[] PROVINCES = {"Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Málaga", "Murcia", "Palma", "Bilbao", "Valladolid"};

    private void createTable() throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("BasicData"));
        family.setMaxVersions(10); // Default is 3.

        admin.createTable(new HTableDescriptor(table).addFamily(family));
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void deleteTable() throws IOException {
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        admin.disableTable(table);
        admin.deleteTable(table);
        admin.close();
        connection.close();
        System.out.println("Table removed");
    }

    private void put(int numberUsers) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        for (int i = 1; i <= numberUsers; i++) {
            // Generate random data for each user
            String name = getRandom(NAMES);
            String province = getRandom(PROVINCES);
            String lastLogin = generateRandomDate();

            byte[] key = Bytes.toBytes(name);
            Put put = new Put(key);

            // Insert data in 'UserInfo' family
            put.addColumn(Bytes.toBytes("BasicData"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("BasicData"), Bytes.toBytes("province"), Bytes.toBytes(province));
            put.addColumn(Bytes.toBytes("BasicData"), Bytes.toBytes("lastLogin"), Bytes.toBytes(lastLogin));

            // Insert row in table
            t.put(put);

            // Print user
            System.out.println("User: name " + name + " province " + province + " lastLogin " + lastLogin);
        }
        t.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    // Auxiliary method to get a random element from an array
    private static String getRandom(String[] array) {
        Random random = new Random();
        return array[random.nextInt(array.length)];
    }

    // Method to generate a random date between 2010 and 2023
    private static String generateRandomDate() {
        Random random = new Random();
        int year = 2010 + random.nextInt(14); // Entre 2010 y 2023
        int month = 1 + random.nextInt(12);   // Entre 1 y 12
        int day = 1 + random.nextInt(28);     // Entre 1 y 28 para simplificar

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(year - 1900, month - 1, day));
    }

    private void delete(String name) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);

        byte[] key = Bytes.toBytes(name);

        Delete delete = new Delete(key);
        t.delete(delete);

        System.out.println("Deleted user: "+name);
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void get(String name) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("province");
        Table t = connection.getTable(table);

        byte[] key = Bytes.toBytes(name);

        Get get = new Get(key);
        Result result = t.get(get);

        String province = Bytes.toString(result.getValue(cf, column));
        System.out.println("User "+ name +" - Province: "+province);
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void getNVersionRow(String name) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column1 = Bytes.toBytes("province");
        byte[] column2 = Bytes.toBytes("lastLogin");

        Table t = connection.getTable(table);

        byte[] key = Bytes.toBytes(name);

        Get get = new Get(key);
        get.setMaxVersions(5);
        Result result = t.get(get);

        List<Cell> valuesColumn1 = result.getColumnCells(cf,column1);
        List<Cell> valuesColumn2 = result.getColumnCells(cf,column2);

        System.out.println("Obtaining all system access for user "+ name+" ...");
        for ( int i = 0; i < valuesColumn1.size(); i++){
            String province = Bytes.toString(CellUtil.cloneValue(valuesColumn1.get(i)));
            String lastLogin = Bytes.toString(CellUtil.cloneValue(valuesColumn2.get(i)));
            System.out.println( "User "+name+" - Province: "+province+ " Last_Login: " +lastLogin);
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void getSpecificColumn(String name) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("lastLogin");
        Table t = connection.getTable(table);

        byte[] key = Bytes.toBytes(name);

        Get get = new Get(key);
        get.addColumn(cf,column);
        Result result = t.get(get);

        String lastLogin = Bytes.toString(result.getValue(cf, column));
        System.out.println("User "+name+" - Last_Login: "+lastLogin);
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }


    private void scan() throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("province");
        Table t = connection.getTable(table);

        Scan scan = new Scan();
        ResultScanner rs = t.getScanner(scan);

        Result result = rs.next();
        while (result!=null && !result.isEmpty()){
            String key = Bytes.toString(result.getRow());
            String province = Bytes.toString(result.getValue(cf,column));
            System.out.println("Key: "+key+" Province: "+province);
            result = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void rangeScan(String name1, String name2) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("province");
        Table t = connection.getTable(table);

        byte[] startKey = Bytes.toBytes(name1);
        byte[] endKey = Bytes.toBytes(name2);

        Scan scan = new Scan(startKey,endKey);
        ResultScanner rs = t.getScanner(scan);

        Result result = rs.next();
        while (result!=null && !result.isEmpty()){
            String key = Bytes.toString(result.getRow());
            String province = Bytes.toString(result.getValue(cf,column));
            System.out.println("Key: "+key+" Province: "+province);
            result = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void filterScan(String province) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("province");
        Table t = connection.getTable(table);

        Scan scan = new Scan();
        Filter f = new SingleColumnValueFilter(cf, column,   CompareFilter.CompareOp.EQUAL,Bytes.toBytes(province));
        scan.setFilter(f);
        ResultScanner rs = t.getScanner(scan);

        Result result = rs.next();
        while (result!=null && !result.isEmpty()){
            String key = Bytes.toString(result.getRow());
            province = Bytes.toString(result.getValue(cf,column));
            System.out.println("Key: "+key+" Province: "+province);
            result = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void split(String splitPoint) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        byte[] splitPoint1 = Bytes.toBytes(splitPoint);

        admin.split(table, splitPoint1);
        waitOnlineNewRegionsAfterSplit(splitPoint1);
        Thread.sleep(30000);

        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void waitOnlineNewRegionsAfterSplit(byte[] startKey) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        HRegionInfo newLeftSideRegion = null;
        HRegionInfo newRightSideRegion = null;

        int retry = 1;
        do {
            Thread.sleep(1000);

            List<HRegionInfo> regions = admin.getTableRegions(table);
            Iterator<HRegionInfo> iter = regions.iterator();

            while (iter.hasNext() && (newLeftSideRegion == null || newRightSideRegion == null)) {
                HRegionInfo rinfo = iter.next();
                if (Arrays.equals(rinfo.getEndKey(), startKey)) {
                    newLeftSideRegion = rinfo;
                }
                if (Arrays.equals(rinfo.getStartKey(), startKey)) {
                    newRightSideRegion = rinfo;
                }
            }
            retry++;
        } while (newLeftSideRegion == null && newRightSideRegion == null && retry <= 50);

        if (retry > 3){
            throw new IOException("split failed, can't find regions with startKey and endKey = "+Bytes.toStringBinary(startKey));
        }
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void move() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        ServerName[] servers = getServers();

        int i = 0;
        for(HRegionInfo hRegionInfo : admin.getTableRegions(table)){
            String originalServer = getServerOfRegion(hRegionInfo).getHostAndPort();
            ServerName finalServer = servers[i];

            if(originalServer.compareTo(finalServer.getHostAndPort())!=0){
                byte[] snb = (finalServer.getHostname() + "," + finalServer.getPort() + "," + finalServer.getStartcode()).getBytes();
                admin.move(hRegionInfo.getEncodedNameAsBytes(), snb);
                wait(10);
            }
            i++;
            if (i == servers.length) {
                i = 0;
            }
        }
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private ServerName[] getServers() throws IOException {
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        Collection<ServerName> serverNames = admin.getClusterStatus().getServers();
        admin.close();
        connection.close();
        return serverNames.toArray(new ServerName[serverNames.size()]);
    }

    private ServerName getServerOfRegion(HRegionInfo hri) throws IOException {
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        for (ServerName sm : getServers()){
            for(HRegionInfo hRegionInfo : admin.getOnlineRegions(sm)){
                if(Bytes.compareTo(hri.getEncodedNameAsBytes(),hRegionInfo.getEncodedNameAsBytes())==0){
                    return sm;
                }
            }
        }
        admin.close();
        connection.close();
        return null;
    }

    private void merge() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        List<HRegionInfo> tableHRs = admin.getTableRegions(table);
        System.out.println("Number of regions: "+tableHRs.size()+" this process will merge the first 2 regions");
        System.out.println(tableHRs.get(0).getEncodedName()+" "+tableHRs.get(1).getEncodedName());
        admin.mergeRegionsAsync(tableHRs.get(0).getRegionName(),tableHRs.get(1).getRegionName(),true).get(30, TimeUnit.SECONDS);
        System.out.println("Number of regions after merge: "+admin.getTableRegions(table).size());
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
        System.out.println("It takes some time to be efective. Check table HFiles using the Hadoop fs tool");
    }

    private void compact() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        try{
            admin.compact(table);
            System.out.println("Compaction done!");
        }catch(Exception e){
            System.out.println(e);
        }
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, TimeoutException {

        Scanner scanner = new Scanner(System.in);
        int input = 0;
        System.out.println("Welcome to the HBase API examples proposed during the Cloud Computing and Big Data Ecosystems course.");
        System.out.println("Next, all possible actions are listed, write the number and press enter to execute the desired action:");
        System.out.println("  1: Create the table Users");
        System.out.println("  2: Load data (PUT)");
        System.out.println("  3: Delete user (DELETE)");
        System.out.println("  4: Get (GET)");
        System.out.println("  5: Get n versions (GET)");
        System.out.println("  6: Get specific column (GET)");
        System.out.println("  7: Scan (SCAN)");
        System.out.println("  8: Range scan (SCAN)");
        System.out.println("  9: Filter scan (SCAN)");
        System.out.println("  10: (ADMIN) split region");
        System.out.println("  11: (ADMIN) move region");
        System.out.println("  12: (ADMIN) merge region");
        System.out.println("  13: (ADMIN) compact region");
        System.out.println("  14: remove table");
        System.out.println("  -1: Exit");
        System.out.println("  -2: Remember actions");

        Main main = new Main();
        String name = "";
        int id=-1;

        // Loop while input different to -1
        while (input != -1) {
            // Read user input
            input = scanner.nextInt();

            // If input is not -1
            if (input != -1) {
                System.out.println("Your input: " + input);
                switch (input) {
                    case 1:
                        main.createTable();
                        System.out.println("Table created");
                        break;
                    case 2:
                        System.out.println("Enter the number of users to create");
                        int numUsers = scanner.nextInt();
                        main.put(numUsers);
                        System.out.println("Users loaded");
                        break;
                    case 3:
                        System.out.println("Enter name: ");
                        name = scanner.next();
                        main.delete(name);
                        break;
                    case 4:
                        System.out.println("Enter name: ");
                        name = scanner.next();
                        main.get(name);
                        break;
                    case 5:
                        System.out.println("Enter name: ");
                        name = scanner.next();
                        main.getNVersionRow(name);
                        break;
                    case 6:
                        System.out.println("Enter name: ");
                        name = scanner.next();
                        main.getSpecificColumn(name);
                        break;
                    case 7:
                        main.scan();
                        break;
                    case 8:
                        System.out.println("Enter first user name: ");
                        name = scanner.next();
                        System.out.println("Enter last user name: ");
                        String name2 = scanner.next();
                        main.rangeScan(name, name2);
                        break;
                    case 9:
                        System.out.println("Enter province to filter: ");
                        String province = scanner.next();
                        main.filterScan(province);
                        break;
                    case 10:
                        System.out.println("Enter user name: ");
                        name = scanner.next();
                        main.split(name);
                        break;
                    case 11:
                        main.move();
                        break;
                    case 12:
                        main.merge();
                        break;
                    case 13:
                        main.compact();
                        break;
                    case 14:
                        main.deleteTable();
                        break;
                    case -2:
                        System.out.println("Next, all possible actions are listed, write the number and press enter to execute the desired action:");
                        System.out.println("  1: Create the table Users");
                        System.out.println("  2: Load data (PUT)");
                        System.out.println("  3: Delete user (DELETE)");
                        System.out.println("  4: Get (GET)");
                        System.out.println("  5: Get n versions (GET)");
                        System.out.println("  6: Get specific column (GET)");
                        System.out.println("  7: Scan (SCAN)");
                        System.out.println("  8: Range scan (SCAN)");
                        System.out.println("  9: Filter scan (SCAN)");
                        System.out.println("  10: (ADMIN) split region");
                        System.out.println("  11: (ADMIN) move region");
                        System.out.println("  12: (ADMIN) merge region");
                        System.out.println("  13: (ADMIN) compact region");
                        System.out.println("  14: remove table");
                        System.out.println("  -1: Exit");
                        System.out.println("  -2: Remember actions");
                        break;
                }

            } else {
                System.out.println("Ending the program...");
            }
        }
    }
}
