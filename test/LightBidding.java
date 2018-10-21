package test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import scala.Tuple2;
import scala.Tuple3;


public class LightBidding {

    private Cluster cluster;
    private Session session;
    private Map<String, List<String>> entityIds;
    private String regionAddress;
    private String categoryAddress;
    private final int usersNumber;
    private final int itemsNumber;
    private final int commentsNumber;
    private final int bidsNumber;
    private final int buysNumber;
    private final int categoryNumber;
    private final int regionNumber;
    private int currentUsersNumber;
    private int currentItemsNumber;
    private int currentBidsNumber;
    private int currentCommentNumber;
    private int currentBuyNumber;
    private Map<String, Long> afm;
    private Map<String, Integer> dr;
    private int readBrowseCategories;
    private int viewBidHistory;
    private int viewItem;
    private int searchItemsByCategory;
    private int viewUserInfo;
    private int registerItem;
    private int registerUser;
    private int buyNow;
    private int storeBuyNow;
    private int putBid;
    private int storeBid;
    private int putComment;
    private int storeComment;
    private int aboutMe;
    private int searchItemsByRegion;
    private int browseRegions;

    public LightBidding() {
        regionAddress = "/home/user/regions.txt";
        categoryAddress = "/home/user/categories.txt";
        usersNumber = 150000;
        itemsNumber = 40000;
        commentsNumber = 110000;
        bidsNumber = 300000;
        buysNumber = 180000;
        categoryNumber = 100;
        regionNumber = 62;
        entityIds = new HashMap<>();
        entityIds.put("table12", new ArrayList<>());
        afm = new HashMap<>();
        dr = new HashMap<>();
        readBrowseCategories = 6617;
        viewBidHistory = 1335;
        viewItem = 12265;
        searchItemsByCategory = 13790;
        viewUserInfo = 2150;
        registerItem = 5;
        registerUser = 9;
        buyNow = 1005;
        storeBuyNow = 10;
        putBid = 4670;
        storeBid = 32;
        putComment = 402;
        storeComment = 4;
        aboutMe = 1477;
        searchItemsByRegion = 5489;
        browseRegions = 1963;

    }

    public static void main(String[] args) throws Exception {

        String baseAdd = "/home/user/lightBidding";

        LightBidding main = new LightBidding();
        main.connect("192.168.82.209");
        main.getSession();
        main.createSchema("lightBidding");
        main.createSchemaAndInsertData();
        main.randomRun(baseAdd);
        main.writeAfmAndDrToFile(baseAdd);
        main.close();
    }

    public void randomRun(String baseAddress) throws Exception {
        fillEntityIds();
        Map<String, Tuple2<BufferedWriter, Integer>> writerMap = new HashMap<>();
        writerMap.put("readBrowseCategories", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/readBrowseCategories.txt")), readBrowseCategories));
        writerMap.put("viewBidHistory", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/viewBidHistory.txt")), viewBidHistory));
        writerMap.put("viewItem", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/viewItem.txt")), viewItem));
        writerMap.put("searchItemsByCategory", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/searchItemsByCategory.txt")), searchItemsByCategory));
        writerMap.put("viewUserInfo", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/viewUserInfo.txt")), viewUserInfo));
        writerMap.put("registerItem", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/registerItem.txt")), registerItem));
        writerMap.put("registerUser", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/registerUser.txt")), registerUser));
        writerMap.put("buyNow", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/buyNow.txt")), buyNow));
        writerMap.put("storeBuyNow", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/storeBuyNow.txt")), storeBuyNow));
        writerMap.put("putBid", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/putBid.txt")), putBid));
        writerMap.put("storeBid", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/storeBid.txt")), storeBid));
        writerMap.put("putComment", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/putComment.txt")), putComment));
        writerMap.put("storeComment", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/storeComment.txt")), storeComment));
        writerMap.put("aboutMe", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/aboutMe.txt")), aboutMe));
        writerMap.put("searchItemsByRegion", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/searchItemsByRegion.txt")), searchItemsByRegion));
        writerMap.put("browseRegions", new Tuple2<>(new BufferedWriter(new FileWriter(baseAddress + "/browseRegions.txt")), browseRegions));
        Map<String, Integer> countMap = new HashMap<>();
        countMap.put("readBrowseCategories", readBrowseCategories);
        countMap.put("viewBidHistory", viewBidHistory);
        countMap.put("viewItem", viewItem);
        countMap.put("searchItemsByCategory", searchItemsByCategory);
        countMap.put("viewUserInfo", viewUserInfo);
        countMap.put("registerItem", registerItem);
        countMap.put("registerUser", registerUser);
        countMap.put("buyNow", buyNow);
        countMap.put("storeBuyNow", storeBuyNow);
        countMap.put("putBid", putBid);
        countMap.put("storeBid", storeBid);
        countMap.put("putComment", putComment);
        countMap.put("storeComment", storeComment);
        countMap.put("aboutMe", aboutMe);
        countMap.put("searchItemsByRegion", searchItemsByRegion);
        countMap.put("browseRegions", browseRegions);
        Map<String, Double> sumMap = new HashMap<>();
        sumMap.put("readBrowseCategories", 0d);
        sumMap.put("viewBidHistory", 0d);
        sumMap.put("viewItem", 0d);
        sumMap.put("searchItemsByCategory", 0d);
        sumMap.put("viewUserInfo", 0d);
        sumMap.put("registerItem", 0d);
        sumMap.put("registerUser", 0d);
        sumMap.put("buyNow", 0d);
        sumMap.put("storeBuyNow", 0d);
        sumMap.put("putBid", 0d);
        sumMap.put("storeBid", 0d);
        sumMap.put("putComment", 0d);
        sumMap.put("storeComment", 0d);
        sumMap.put("aboutMe", 0d);
        sumMap.put("searchItemsByRegion", 0d);
        sumMap.put("browseRegions", 0d);

        Random random = new Random();
        while (!countMap.isEmpty()) {
            String st = String.valueOf(countMap.keySet().toArray()[random.nextInt(countMap.keySet().toArray().length)]);
            if (countMap.get(st) == 0) {
                countMap.remove(st);
                writerMap.get(st)._1().write("======================================\nAvg Time: ");
                double avg = sumMap.get(st) / writerMap.get(st)._2();
                writerMap.get(st)._1().write(String.valueOf(avg));
                writerMap.get(st)._1().close();
                System.out.println(st + " finished!");
                continue;
            }
            switch (st) {
                case "readBrowseCategories":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readBrowseCategories(1, writerMap.get(st)._1()));
                    break;
                case "viewBidHistory":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readViewBidHistory(1, writerMap.get(st)._1()));
                    break;
                case "viewItem":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readViewItem(1, writerMap.get(st)._1()));
                    break;
                case "searchItemsByCategory":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readSearchItemsByCategory(1, writerMap.get(st)._1()));
                    break;
                case "viewUserInfo":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readViewUserInfo(1, writerMap.get(st)._1()));
                    break;
                case "registerItem":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readRegisterItem(1, writerMap.get(st)._1()));
                    break;
                case "registerUser":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readRegisterUser(1, writerMap.get(st)._1()));
                    break;
                case "buyNow":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readBuyNow(1, writerMap.get(st)._1()));
                    break;
                case "storeBuyNow":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readStoreBuyNow(1, writerMap.get(st)._1()));
                    break;
                case "putBid":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readPutBid(1, writerMap.get(st)._1()));
                    break;
                case "storeBid":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readStoreBid(1, writerMap.get(st)._1()));
                    break;
                case "putComment":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readPutComment(1, writerMap.get(st)._1()));
                    break;
                case "storeComment":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readStoreComment(1, writerMap.get(st)._1()));
                    break;
                case "aboutMe":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readAboutMe(1, writerMap.get(st)._1()));
                    break;
                case "searchItemsByRegion":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readSearchItemsByRegion(1, writerMap.get(st)._1()));
                    break;
                case "browseRegions":
                    countMap.put(st, countMap.get(st) - 1);
                    sumMap.put(st, sumMap.get(st) + readBrowseRegions(1, writerMap.get(st)._1()));
                    break;
            }
        }

    }

    public void createSchemaAndInsertData() throws Exception {
        createSchema("lightBidding");
        loadUserQueryToCassandra(usersNumber, false);
        loadCommentsQueryToCassandra(commentsNumber);
        loadItemQueryToCassandra(itemsNumber);
        loadBuyNowQueryToCassandra(buysNumber);
        loadBidsQueryToCassandra(bidsNumber, usersNumber, itemsNumber);
    }

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.println("Connected to cluster:" + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.println("Datatacenter: " + host.getDatacenter()
                    + "; Host: " + host.getAddress() + "; Rack: "
                    + host.getRack());
        }
    }

    public void getSession() {
        session = cluster.connect();
    }

    public void closeSession() {
        session.close();
    }

    public void close() {
        cluster.close();
    }

    public void fillEntityIds() {
        String q1 = "SELECT users0id FROM lightBidding.table5;";
        ResultSet results1 = session.execute(q1);
        currentUsersNumber = -1;
        for (Row row : results1) {
            if (currentUsersNumber < Integer.valueOf(row.getString(0).toLowerCase().replaceAll("users0id", ""))) {
                currentUsersNumber = Integer.valueOf(row.getString(0).toLowerCase().replaceAll("users0id", ""));
            }
        }
        q1 = "SELECT items0id FROM lightBidding.table3;";
        results1 = session.execute(q1);
        currentItemsNumber = -1;
        for (Row row : results1) {
            if (currentItemsNumber < Integer.valueOf(row.getString(0).replaceAll("items0id", ""))) {
                currentItemsNumber = Integer.valueOf(row.getString(0).replaceAll("items0id", ""));
            }
        }

        q1 = "SELECT bids0id FROM lightBidding.table2;";
        results1 = session.execute(q1);
        currentBidsNumber = -1;
        for (Row row : results1) {
            if (currentBidsNumber < Integer.valueOf(row.getString(0).replaceAll("bids0id", ""))) {
                currentBidsNumber = Integer.valueOf(row.getString(0).replaceAll("bids0id", ""));
            }
        }
        q1 = "SELECT comments0id FROM lightBidding.table8;";
        results1 = session.execute(q1);
        currentCommentNumber = -1;
        for (Row row : results1) {
            if (currentCommentNumber < Integer.valueOf(row.getString(0).replaceAll("comments0id", ""))) {
                currentCommentNumber = Integer.valueOf(row.getString(0).replaceAll("comments0id", ""));
            }
        }

        q1 = "SELECT regions0id, categories0id FROM lightBidding.table12;";
        ResultSet results = session.execute(q1);
        for (Row row : results) {
            entityIds.get("table12").add(row.getString(0) + ":" + row.getString(1));
        }
    }

    public void createSchema(String ksName) throws Exception {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use " + ksName);
        session.execute("CREATE TABLE  IF NOT EXISTS table1 (categories0dummy text,categories0id text,categories0name text, primary KEY (categories0dummy,categories0id));");
        session.execute("CREATE TABLE  IF NOT EXISTS table2 (items0id text, bids0id text, bids0date text, users0id text,users0nickname text, bids0bid text, bids0qty text, primary KEY (items0id, bids0id, bids0date, users0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table3 (items0id text, items0buy_now text, items0name text, items0max_bid text, items0quantity text, items0end_date text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (items0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table4 (categories0id text,items0id text, items0end_date text,items0buy_now text, items0name text, items0max_bid text, items0quantity text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (categories0id, items0end_date, items0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table5 (users0id text,users0email text, users0nickname text, users0lastname text, users0rating text, users0creation_date text, users0firstname text, users0password text, users0balance text, primary KEY (users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table6 (users0id text,comments0id text,comments0comment text, comments0date text, comments0rating text, primary KEY (users0id,comments0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table7 (users0id text,comments0id text ,comments0comment text, comments0date text, comments0rating text, primary KEY (users0id,comments0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table8 (comments0id text,users0id text,users0nickname text, primary KEY (comments0id,users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table9 (users0id text,buy_now0date text, items0id text, buy_now0id text,items0buy_now text, items0name text, items0max_bid text, buy_now0qty text, items0quantity text, items0end_date text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (users0id, buy_now0date, items0id,  buy_now0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table10 (users0id text,items0id text, items0end_date text,items0buy_now text, items0name text, items0max_bid text, items0quantity text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (users0id, items0end_date, items0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table11 (users0id text,items0id text, bids0id text, items0end_date text,items0buy_now text, items0name text, items0max_bid text, items0quantity text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (users0id, items0end_date,items0id,  bids0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table12 (regions0id text, categories0id text,items0id text, items0end_date text, users0id text,items0buy_now text, items0name text, items0max_bid text, items0quantity text, items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text, primary KEY (regions0id, categories0id, items0end_date, items0id, users0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table13 (regions0dummy text,regions0id text,regions0name text, primary KEY (regions0dummy,regions0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable14 (users0id text,regions0id text, primary KEY (users0id,regions0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable15 (items0id text,categories0id text,items0end_date text, primary KEY (items0id,categories0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable16 (items0id text,buy_now0id text, users0id text,buy_now0date text, primary KEY (items0id,buy_now0id,users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable18 (items0id text,bids0id text, users0id text,items0end_date text, primary KEY (items0id,bids0id,users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable19 (items0id text,regions0id text, categories0id text, users0id text,items0end_date text, primary KEY (items0id,regions0id, categories0id, users0id)) ;");
        session.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_table2 AS select items0id, bids0bid, bids0id, bids0date, users0id, bids0date, bids0qty from table2 WHERE bids0bid IS NOT NULL AND bids0id IS NOT NULL AND bids0date IS NOT NULL AND users0id IS NOT NULL AND items0id IS NOT NULL PRIMARY KEY(items0id,bids0bid, bids0id, bids0date, users0id);");
        insertRegions();
        insertCategories();
    }

    public int insertRegions() throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(regionAddress));
        String line;
        int i = 0;
        while ((line = reader.readLine()) != null) {
            if (line.length() > 2) {
                i++;
                session.execute("INSERT INTO table13 (regions0dummy ,regions0id ,regions0name) VALUES ('0','" + i + "','" + line + "');");
            }
        }
        reader.close();
        return i;
    }

    public int insertCategories() throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(categoryAddress));
        String line;
        int i = 0;
        while ((line = reader.readLine()) != null) {
            if (line.length() > 2) {
                i++;
                session.execute("INSERT INTO table1 (categories0dummy,categories0id ,categories0name) VALUES ('0','" + i + "','" + line + "');");
            }
        }
        reader.close();
        return i;
    }

    public void insertRecords(String tablename, String feilds, Map<String, String> data) throws Exception {

        for (String key : data.keySet()) {
//            System.out.println("INSERT INTO " + tablename + " (" + feilds + ") " + "VALUES (" + data.get(key) + ");");
            session.execute("INSERT INTO " + tablename + " (" + feilds + ") " + "VALUES (" + data.get(key) + ");");
        }
    }

    public Long insertRecord(String tablename, String feilds, String data) throws Exception {
        long s = new Date().getTime();
//        String q = "INSERT INTO lightBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");";
//        System.out.println("INSERT INTO lightBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        session.execute("INSERT INTO lightBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        long e = new Date().getTime();
        return e - s;
    }

    public void loadItemQueryToCassandra(int itemCount) throws Exception {
        for (int i = 0; i < itemCount; i++) {
            int userRand = getRandomNumber(usersNumber);
            int catRand = getRandomNumber(categoryNumber);
            // insert into table 3
            String feilds = "items0id, items0buy_now, items0description, items0end_date, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
            StringBuilder builder = new StringBuilder();
            builder.append("'items0id").append(i).append("',");
            builder.append("'items0buy_now").append(i).append("',");
            builder.append("'items0description").append(i).append("',");
            builder.append("'items0end_date").append(i).append("',");
            builder.append("'items0initial_price").append(i).append("',");
            builder.append("'items0max_bid").append(i).append("',");
            builder.append("'items0name").append(i).append("',");
            builder.append("'items0nb_of_bids").append(i).append("',");
            builder.append("'items0quantity").append(i).append("',");
            builder.append("'items0reserve_price").append(i).append("',");
            builder.append("'items0start_date").append(i).append("'");
            insertRecord("table3", feilds, builder.toString());
            // insert into table 4
            feilds = "categories0id,items0id,items0end_date,items0buy_now,items0description,items0initial_price,items0max_bid,items0name,items0nb_of_bids,items0quantity,items0reserve_price,items0start_date";
            StringBuilder builderTbl4 = new StringBuilder();
            builderTbl4.append("'categories0id").append(catRand).append("',");
            builderTbl4.append("'items0id").append(i).append("',");
            builderTbl4.append("'items0end_date").append(i).append("',");
            builderTbl4.append("'items0buy_now").append(i).append("',");
            builderTbl4.append("'items0description").append(i).append("',");
            builderTbl4.append("'items0initial_price").append(i).append("',");
            builderTbl4.append("'items0max_bid").append(i).append("',");
            builderTbl4.append("'items0name").append(i).append("',");
            builderTbl4.append("'items0nb_of_bids").append(i).append("',");
            builderTbl4.append("'items0quantity").append(i).append("',");
            builderTbl4.append("'items0reserve_price").append(i).append("',");
            builderTbl4.append("'items0start_date").append(i).append("'");
            insertRecord("table4", feilds, builderTbl4.toString());
            // insert into table 10
            feilds = "users0id, items0id, items0end_date, items0buy_now, items0description, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
//            entityIds.get("table10:users").add(String.valueOf(userRand));
            StringBuilder builderTbl10 = new StringBuilder();
            builderTbl10.append("'users0id").append(userRand).append("',");
            builderTbl10.append("'items0id").append(i).append("',");
            builderTbl10.append("'items0end_date").append(i).append("',");
            builderTbl10.append("'items0buy_now").append(i).append("',");
            builderTbl10.append("'items0description").append(i).append("',");
            builderTbl10.append("'items0initial_price").append(i).append("',");
            builderTbl10.append("'items0max_bid").append(i).append("',");
            builderTbl10.append("'items0name").append(i).append("',");
            builderTbl10.append("'items0nb_of_bids").append(i).append("',");
            builderTbl10.append("'items0quantity").append(i).append("',");
            builderTbl10.append("'items0reserve_price").append(i).append("',");
            builderTbl10.append("'items0start_date").append(i).append("'");
            insertRecord("table10", feilds, builderTbl10.toString());
            // insert into table 12
            feilds = "regions0id, categories0id, items0id, items0end_date, users0id, items0buy_now, items0description, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
            StringBuilder builderTbl12 = new StringBuilder();
//            entityIds.get("table12:users").add(String.valueOf(userRand));
            String regions0id = getRecord("stable14", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");
            builderTbl12.append("'").append(regions0id).append("',");
            builderTbl12.append("'categories0id").append(catRand).append("',");
            builderTbl12.append("'items0id").append(i).append("',");
            builderTbl12.append("'items0end_date").append(i).append("',");
            builderTbl12.append("'users0id").append(userRand).append("',");
            builderTbl12.append("'items0buy_now").append(i).append("',");
            builderTbl12.append("'items0description").append(i).append("',");
            builderTbl12.append("'items0initial_price").append(i).append("',");
            builderTbl12.append("'items0max_bid").append(i).append("',");
            builderTbl12.append("'items0name").append(i).append("',");
            builderTbl12.append("'items0nb_of_bids").append(i).append("',");
            builderTbl12.append("'items0quantity").append(i).append("',");
            builderTbl12.append("'items0reserve_price").append(i).append("',");
            builderTbl12.append("'items0start_date").append(i).append("'");
            insertRecord("table12", feilds, builderTbl12.toString());

            // insert into table 15
            feilds = "items0id, items0end_date,categories0id";
            StringBuilder builderTbl15 = new StringBuilder();
            builderTbl15.append("'items0id").append(i).append("',");
            builderTbl15.append("'items0end_date").append(i).append("',");
            builderTbl15.append("'categories0id").append(catRand).append("'");
            insertRecord("stable15", feilds, builderTbl15.toString());
            // insert into table 19
            feilds = "items0id,regions0id, categories0id, users0id, items0end_date";
            StringBuilder builderTbl19 = new StringBuilder();
            builderTbl19.append("'items0id").append(i).append("',");
            builderTbl19.append("'regions0id").append(regions0id).append("',");
            builderTbl19.append("'categories0id").append(catRand).append("',");
            builderTbl19.append("'users0id").append(userRand).append("',");
            builderTbl19.append("'items0end_date").append(i).append("'");
            insertRecord("stable19", feilds, builderTbl19.toString());
        }
    }

    public Long insertItemQuery() throws Exception {
        long d = 0;
        int userRand = getRandomNumber(usersNumber);
        int catRand = getRandomNumber(categoryNumber);
        currentItemsNumber++;
        int i = currentItemsNumber;
        // insert into table 3
        String feilds = "items0id, items0buy_now, items0description, items0end_date, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
        StringBuilder builder = new StringBuilder();
        builder.append("'items0id").append(i).append("',");
        builder.append("'items0buy_now").append(i).append("',");
        builder.append("'items0description").append(i).append("',");
        builder.append("'items0end_date").append(i).append("',");
        builder.append("'items0initial_price").append(i).append("',");
        builder.append("'items0max_bid").append(i).append("',");
        builder.append("'items0name").append(i).append("',");
        builder.append("'items0nb_of_bids").append(i).append("',");
        builder.append("'items0quantity").append(i).append("',");
        builder.append("'items0reserve_price").append(i).append("',");
        builder.append("'items0start_date").append(i).append("'");
        d += (insertRecord("table3", feilds, builder.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table3", 1);

        // insert into table 4
        feilds = "categories0id,items0id,items0end_date,items0buy_now,items0description,items0initial_price,items0max_bid,items0name,items0nb_of_bids,items0quantity,items0reserve_price,items0start_date";
        StringBuilder builderTbl4 = new StringBuilder();
        builderTbl4.append("'categories0id").append(catRand).append("',");
        builderTbl4.append("'items0id").append(i).append("',");
        builderTbl4.append("'items0end_date").append(i).append("',");
        builderTbl4.append("'items0buy_now").append(i).append("',");
        builderTbl4.append("'items0description").append(i).append("',");
        builderTbl4.append("'items0initial_price").append(i).append("',");
        builderTbl4.append("'items0max_bid").append(i).append("',");
        builderTbl4.append("'items0name").append(i).append("',");
        builderTbl4.append("'items0nb_of_bids").append(i).append("',");
        builderTbl4.append("'items0quantity").append(i).append("',");
        builderTbl4.append("'items0reserve_price").append(i).append("',");
        builderTbl4.append("'items0start_date").append(i).append("'");
        d += (insertRecord("table4", feilds, builderTbl4.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table4", 1);

        // insert into table 10
        feilds = "users0id, items0id, items0end_date, items0buy_now, items0description, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
//        entityIds.get("table10:users").add(String.valueOf(userRand));
        StringBuilder builderTbl10 = new StringBuilder();
        builderTbl10.append("'users0id").append(userRand).append("',");
        builderTbl10.append("'items0id").append(i).append("',");
        builderTbl10.append("'items0end_date").append(i).append("',");
        builderTbl10.append("'items0buy_now").append(i).append("',");
        builderTbl10.append("'items0description").append(i).append("',");
        builderTbl10.append("'items0initial_price").append(i).append("',");
        builderTbl10.append("'items0max_bid").append(i).append("',");
        builderTbl10.append("'items0name").append(i).append("',");
        builderTbl10.append("'items0nb_of_bids").append(i).append("',");
        builderTbl10.append("'items0quantity").append(i).append("',");
        builderTbl10.append("'items0reserve_price").append(i).append("',");
        builderTbl10.append("'items0start_date").append(i).append("'");
        d += (insertRecord("table10", feilds, builderTbl10.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table10", 1);
        // insert into table 12
        feilds = "regions0id, categories0id, items0id, items0end_date, users0id, items0buy_now, items0description, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
        StringBuilder builderTbl12 = new StringBuilder();
//        entityIds.get("table12:users").add(String.valueOf(userRand));
        String regions0id = getRecord("stable14", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");
        builderTbl12.append("'").append(regions0id).append("',");
        builderTbl12.append("'categories0id").append(catRand).append("',");
        builderTbl12.append("'items0id").append(i).append("',");
        builderTbl12.append("'items0end_date").append(i).append("',");
        builderTbl12.append("'users0id").append(userRand).append("',");
        builderTbl12.append("'items0buy_now").append(i).append("',");
        builderTbl12.append("'items0description").append(i).append("',");
        builderTbl12.append("'items0initial_price").append(i).append("',");
        builderTbl12.append("'items0max_bid").append(i).append("',");
        builderTbl12.append("'items0name").append(i).append("',");
        builderTbl12.append("'items0nb_of_bids").append(i).append("',");
        builderTbl12.append("'items0quantity").append(i).append("',");
        builderTbl12.append("'items0reserve_price").append(i).append("',");
        builderTbl12.append("'items0start_date").append(i).append("'");
        d += (insertRecord("table12", feilds, builderTbl12.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table12", 1);

        // insert into table 15
        feilds = "items0id, items0end_date,categories0id";
        StringBuilder builderTbl15 = new StringBuilder();
        builderTbl15.append("'items0id").append(i).append("',");
        builderTbl15.append("'items0end_date").append(i).append("',");
        builderTbl15.append("'categories0id").append(catRand).append("'");
        d += (insertRecord("stable15", feilds, builderTbl15.toString()));
        calculateAFM("RegisterItem:stable15", 1);
        calculateDr(feilds);

        // insert into table 19
        feilds = "items0id,regions0id, categories0id, users0id, items0end_date";
        StringBuilder builderTbl19 = new StringBuilder();
        builderTbl19.append("'items0id").append(i).append("',");
        builderTbl19.append("'regions0id").append(regions0id).append("',");
        builderTbl19.append("'categories0id").append(catRand).append("',");
        builderTbl19.append("'users0id").append(userRand).append("',");
        builderTbl19.append("'items0end_date").append(i).append("'");

        d += (insertRecord("stable19", feilds, builderTbl19.toString()));
        calculateAFM("RegisterItem:stable19", 1);
        calculateDr(feilds);
        return d;
    }

    public void loadBuyNowQueryToCassandra(int buyNumber) throws Exception {
        for (int i = 0; i < buyNumber; i++) {
            int userRand = getRandomNumber(usersNumber);
            int itemRand = getRandomNumber(itemsNumber);
            // insert into table 9
            String feilds = "users0id, buy_now0date, items0id, buy_now0id, buy_now0qty, items0buy_now, items0description, items0end_date, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
            StringBuilder builderTbl9 = new StringBuilder();
            Map<String, String> map = getRecord("table3", "items0id", "items0id" + String.valueOf(itemRand));
            builderTbl9.append("'users0id").append(userRand).append("',");
            builderTbl9.append("'buy_now0date").append(i).append("',");
            builderTbl9.append("'items0id").append(itemRand).append("',");
            builderTbl9.append("'buy_now0id").append(i).append("',");
            builderTbl9.append("'buy_now0qty").append(i).append("',");
            builderTbl9.append("'").append(map.get("items0buy_now")).append("',");
            builderTbl9.append("'").append(map.get("items0description")).append("',");
            builderTbl9.append("'").append(map.get("items0end_date")).append("',");
            builderTbl9.append("'").append(map.get("items0initial_price")).append("',");
            builderTbl9.append("'").append(map.get("items0max_bid")).append("',");
            builderTbl9.append("'").append(map.get("items0name")).append("',");
            builderTbl9.append("'").append(map.get("items0nb_of_bids")).append("',");
            builderTbl9.append("'").append(map.get("items0quantity")).append("',");
            builderTbl9.append("'").append(map.get("items0reserve_price")).append("',");
            builderTbl9.append("'").append(map.get("items0start_date")).append("'");
            insertRecord("table9", feilds, builderTbl9.toString());

            // insert into stable 16
            feilds = "items0id,buy_now0id, users0id, buy_now0date";
            StringBuilder builderTbl16 = new StringBuilder();
            builderTbl16.append("'items0id").append(itemRand).append("',");
            builderTbl16.append("'buy_now0id").append(i).append("',");
            builderTbl16.append("'users0id").append(userRand).append("',");
            builderTbl16.append("'buy_now0date").append(i).append("'");
            insertRecord("stable16", feilds, builderTbl16.toString());
        }
    }

    public Long insertBuyNowQuery(String itemId) throws Exception {
        long times = 0;
        currentBuyNumber++;
        int buyRand = currentBuyNumber;
        int userRand = getRandomNumber(usersNumber);
        // insert into table 9
        Tuple3<Long, List<Row>, Integer> tuple3 = execRecord("select * from lightBidding.table3 where items0id ='" + itemId + "';");
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            calculateAFM("StoreBuyNow:table3", 1);
            String feilds = "users0id, buy_now0date, items0id, buy_now0id, buy_now0qty, items0buy_now, items0description, items0end_date, items0initial_price, items0max_bid, items0name, items0nb_of_bids, items0quantity, items0reserve_price, items0start_date";
            StringBuilder builderTbl9 = new StringBuilder();
            builderTbl9.append("'users0id").append(buyRand).append("',");
            builderTbl9.append("'buy_now0date").append(buyRand).append("',");
            builderTbl9.append("'").append(map.get("items0id")).append("',");
            builderTbl9.append("'buy_now0id").append(buyRand).append("',");
            builderTbl9.append("'buy_now0qty").append(buyRand).append("',");
            builderTbl9.append("'").append(map.get("items0buy_now")).append("',");
            builderTbl9.append("'").append(map.get("items0description")).append("',");
            builderTbl9.append("'").append(map.get("items0end_date")).append("',");
            builderTbl9.append("'").append(map.get("items0initial_price")).append("',");
            builderTbl9.append("'").append(map.get("items0max_bid")).append("',");
            builderTbl9.append("'").append(map.get("items0name")).append("',");
            builderTbl9.append("'").append(map.get("items0nb_of_bids")).append("',");
            builderTbl9.append("'").append(map.get("items0quantity")).append("',");
            builderTbl9.append("'").append(map.get("items0reserve_price")).append("',");
            builderTbl9.append("'").append(map.get("items0start_date")).append("'");
            times += (insertRecord("table9", feilds, builderTbl9.toString()));
            calculateDr(feilds);
            calculateAFM("StoreBuyNow:table9", 1);

            // insert into table 16
            feilds = "items0id,buy_now0id, users0id, buy_now0date";
            StringBuilder builderTbl16 = new StringBuilder();
            builderTbl16.append("'").append(map.get("items0id")).append("',");
            builderTbl16.append("'buy_now0id").append(buyRand).append("',");
            builderTbl16.append("'users0id").append(userRand).append("',");
            builderTbl16.append("'buy_now0date").append(buyRand).append("'");
            times += (insertRecord("stable16", feilds, builderTbl16.toString()));
            calculateAFM("StoreBuyNow:stable16", 1);
            calculateDr(feilds);
        }

        return times;
    }

    public void loadBidsQueryToCassandra(int bidNumber, int userNumber, int itemNumber) throws Exception {
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < bidNumber; i++) {
            int itemRand = getRandomNumber(itemNumber);
            int userRand = getRandomNumber(userNumber);
            // insert into table 2
            String feilds = "items0id, bids0id,bids0date,users0id, bids0bid,bids0qty,users0nickname";
            String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(userRand)).get("users0nickname");
            StringBuilder builderTbl2 = new StringBuilder();
            builderTbl2.append("'items0id").append(itemRand).append("',");
            builderTbl2.append("'bids0id").append(i).append("',");
            builderTbl2.append("'bids0date").append(i).append("',");
            builderTbl2.append("'users0id").append(userRand).append("',");
            builderTbl2.append("'bids0bid").append(i).append("',");
            builderTbl2.append("'bids0qty").append(i).append("',");
            builderTbl2.append("'users0nickname").append(users0nickname).append("'");
            times.add(insertRecord("table2", feilds, builderTbl2.toString()));

            // insert into table 11
            feilds = "users0id, items0id, bids0id,items0end_date,items0buy_now,items0description,items0initial_price,items0max_bid,items0name,items0nb_of_bids,items0quantity,items0reserve_price,items0start_date";
            Map<String, String> map = getRecord("table3", "items0id", "items0id" + String.valueOf(itemRand));
//            entityIds.get("table11:items").add(String.valueOf(itemRand));
//            entityIds.get("table11:users").add(String.valueOf(userRand));
            StringBuilder builderTbl11 = new StringBuilder();
            builderTbl11.append("'users0id").append(userRand).append("',");
            builderTbl11.append("'items0id").append(itemRand).append("',");
            builderTbl11.append("'bids0id").append(i).append("',");
            builderTbl11.append("'").append(map.get("items0buy_now")).append("',");
            builderTbl11.append("'").append(map.get("items0description")).append("',");
            builderTbl11.append("'").append(map.get("items0end_date")).append("',");
            builderTbl11.append("'").append(map.get("items0initial_price")).append("',");
            builderTbl11.append("'").append(map.get("items0max_bid")).append("',");
            builderTbl11.append("'").append(map.get("items0name")).append("',");
            builderTbl11.append("'").append(map.get("items0nb_of_bids")).append("',");
            builderTbl11.append("'").append(map.get("items0quantity")).append("',");
            builderTbl11.append("'").append(map.get("items0reserve_price")).append("',");
            builderTbl11.append("'").append(map.get("items0start_date")).append("'");
            times.add(insertRecord("table11", feilds, builderTbl11.toString()));

            // insert into table 18
            feilds = "items0id,bids0id, users0id, items0end_date";
            String items0end_date = getRecord("table3", "items0id", "items0id" + String.valueOf(itemRand)).get("items0end_date");
//            entityIds.get("stable18:items").add(String.valueOf(itemRand));
//            entityIds.get("stable18:users").add(String.valueOf(userRand));
            StringBuilder builderTbl18 = new StringBuilder();
            builderTbl18.append("'items0id").append(itemRand).append("',");
            builderTbl18.append("'bids0id").append(i).append("',");
            builderTbl18.append("'users0id").append(userRand).append("',");
            builderTbl18.append("'").append(items0end_date).append("'");
            times.add(insertRecord("stable18", feilds, builderTbl18.toString()));
        }
    }

    public Long insertBidsQuery(Map<String, String> map) throws Exception {
        long times = 0;
        // insert into table 2
        currentBidsNumber++;
        int bidRand = currentBidsNumber;
        int userRand = getRandomNumber(usersNumber);
        String feilds = "items0id, bids0id,bids0date,users0id, bids0bid,bids0qty,users0nickname";
        String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(userRand)).get("users0nickname");
        calculateAFM("StoreBid:table5", 1);
        StringBuilder builderTbl2 = new StringBuilder();
        builderTbl2.append("'items0id").append(map.get("items0id")).append("',");
        builderTbl2.append("'bids0id").append(bidRand).append("',");
        builderTbl2.append("'bids0date").append(bidRand).append("',");
        builderTbl2.append("'users0id").append(userRand).append("',");
        builderTbl2.append("'bids0bid").append(bidRand).append("',");
        builderTbl2.append("'bids0qty").append(bidRand).append("',");
        builderTbl2.append("'users0nickname").append(users0nickname).append("'");
        times += (insertRecord("table2", feilds, builderTbl2.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBid:table2", 1);

        // insert into table 11
        feilds = "users0id, items0id, bids0id,items0end_date,items0buy_now,items0description,items0initial_price,items0max_bid,items0name,items0nb_of_bids,items0quantity,items0reserve_price,items0start_date";
        Map<String, String> items = getRecord("table3", "items0id", "items0id" + map.get("items0id"));
        calculateAFM("StoreBid:table3", 1);
        StringBuilder builderTbl11 = new StringBuilder();
        builderTbl11.append("'users0id").append(userRand).append("',");
        builderTbl11.append("'items0id").append(map.get("items0id")).append("',");
        builderTbl11.append("'bids0id").append(bidRand).append("',");
        builderTbl11.append("'").append(items.get("items0buy_now")).append("',");
        builderTbl11.append("'").append(items.get("items0description")).append("',");
        builderTbl11.append("'").append(items.get("items0end_date")).append("',");
        builderTbl11.append("'").append(items.get("items0initial_price")).append("',");
        builderTbl11.append("'").append(items.get("items0max_bid")).append("',");
        builderTbl11.append("'").append(items.get("items0name")).append("',");
        builderTbl11.append("'").append(items.get("items0nb_of_bids")).append("',");
        builderTbl11.append("'").append(items.get("items0quantity")).append("',");
        builderTbl11.append("'").append(items.get("items0reserve_price")).append("',");
        builderTbl11.append("'").append(items.get("items0start_date")).append("'");
        times += (insertRecord("table11", feilds, builderTbl11.toString()));
        calculateAFM("StoreBid:table11", 1);
        calculateDr(feilds);

        // insert into table 18
        feilds = "items0id,bids0id, users0id, items0end_date";
        StringBuilder builderTbl18 = new StringBuilder();
        builderTbl18.append("'items0id").append(map.get("items0id")).append("',");
        builderTbl18.append("'bids0id").append(bidRand).append("',");
        builderTbl18.append("'users0id").append(userRand).append("',");
        builderTbl18.append("'items0end_date").append(items.get("items0end_date")).append("'");
        times += (insertRecord("stable18", feilds, builderTbl18.toString()));
        calculateAFM("StoreBid:stable18", 1);
        calculateDr(feilds);
        return times;
    }

    public void loadCommentsQueryToCassandra(int commentCount) throws Exception {
        // insert into table 6
        for (int i = 0; i < commentCount; i++) {
            int userRand = getRandomNumber(usersNumber);
            String feilds = "users0id,comments0id, comments0comment,comments0date, comments0rating";
            StringBuilder builderTbl6 = new StringBuilder();
            builderTbl6.append("'users0id").append(userRand).append("',");
            builderTbl6.append("'comments0id").append(i).append("',");
            builderTbl6.append("'comments0comment").append(i).append("',");
            builderTbl6.append("'comments0date").append(i).append("',");
            builderTbl6.append("'comments0rating").append(i).append("'");
            insertRecord("table6", feilds, builderTbl6.toString());

            // insert into table 7
            feilds = "users0id, comments0id,comments0comment,comments0date, comments0rating ";
            StringBuilder builderTbl7 = new StringBuilder();
            builderTbl7.append("'users0id").append(userRand).append("',");
            builderTbl7.append("'comments0id").append(i).append("',");
            builderTbl7.append("'comments0comment").append(i).append("',");
            builderTbl7.append("'comments0date").append(i).append("',");
            builderTbl7.append("'comments0rating").append(i).append("'");
            insertRecord("table7", feilds, builderTbl7.toString());

            // insert into table 8
            feilds = " comments0id,users0id,users0nickname";
            String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(userRand)).get("users0nickname");
            StringBuilder builderTbl8 = new StringBuilder();
            builderTbl8.append("'comments0id").append(i).append("',");
            builderTbl8.append("'users0id").append(userRand).append("',");
            builderTbl8.append("'users0nickname").append(users0nickname).append("'");
            insertRecord("table8", feilds, builderTbl8.toString());
        }
    }

    public Long insertCommentsQuery(Map<String, String> map) throws Exception {
        long times = 0;
        // insert into table 6
        currentCommentNumber++;
        int commentRand = currentCommentNumber;
        String feilds = "users0id,comments0id, comments0comment,comments0date, comments0rating";
        StringBuilder builderTbl6 = new StringBuilder();
        builderTbl6.append("'users0id").append(map.get("users0id")).append("',");
        builderTbl6.append("'comments0id").append(commentRand).append("',");
        builderTbl6.append("'comments0comment").append(commentRand).append("',");
        builderTbl6.append("'comments0date").append(commentRand).append("',");
        builderTbl6.append("'comments0rating").append(commentRand).append("'");
        times += (insertRecord("table6", feilds, builderTbl6.toString()));
        calculateDr(feilds);
        calculateAFM("StoreComment:table6", 1);

        // insert into table 7
        feilds = "users0id, comments0id,comments0comment,comments0date, comments0rating ";
        StringBuilder builderTbl7 = new StringBuilder();
        builderTbl7.append("'users0id").append(map.get("users0id")).append("',");
        builderTbl7.append("'comments0id").append(commentRand).append("',");
        builderTbl7.append("'comments0comment").append(commentRand).append("',");
        builderTbl7.append("'comments0date").append(commentRand).append("',");
        builderTbl7.append("'comments0rating").append(commentRand).append("'");
        times += (insertRecord("table7", feilds, builderTbl7.toString()));
        calculateDr(feilds);
        calculateAFM("StoreComment:table7", 1);

        // insert into table 8
        feilds = " comments0id,users0id,users0nickname";
        String users0nickname = getRecord("table5", "users0id", "Users0id" + String.valueOf(map.get("users0id"))).get("users0nickname");
        calculateAFM("StoreComment:table5", 1);
        StringBuilder builderTbl8 = new StringBuilder();
        builderTbl8.append("'comments0id").append(commentRand).append("',");
        builderTbl8.append("'users0id").append(map.get("users0id")).append("',");
        builderTbl8.append("'users0nickname").append(users0nickname).append("'");
        times += (insertRecord("table8", feilds, builderTbl8.toString()));
        calculateDr(feilds);
        calculateAFM("StoreComment:table8", 1);
        return times;
    }

    public void loadUserQueryToCassandra(int userCount, boolean flag) throws Exception {
        //   Insert to Table5
        for (int i = 0; i < userCount; i++) {
            String feilds = "users0id,users0firstname,users0lastname,"
                    + " users0nickname, users0password,"
                    + " users0email, users0rating, users0balance,"
                    + " users0creation_date";
            StringBuilder builder5 = new StringBuilder();
            builder5.append("'Users0id").append(i).append("',");
            builder5.append("'Users0firstname").append(i).append("',");
            builder5.append("'Users0lastname").append(i).append("',");
            builder5.append("'Users0nickname").append(i).append("',");
            builder5.append("'Users0password").append(i).append("',");
            builder5.append("'Users0email").append(i).append("',");
            builder5.append("'0").append("',");
            builder5.append("'0").append("',");
            builder5.append("'Users0Creation_date").append(i).append("'");
            insertRecord("table5", feilds, builder5.toString());

            // insert into stable 14
            feilds = "regions0id,users0id";
            StringBuilder builderTbl14 = new StringBuilder();
            builderTbl14.append("'regions0id").append(getRandomNumber(regionNumber)).append("',");
            builderTbl14.append("'users0id").append(i).append("'");
            insertRecord("stable14", feilds, builderTbl14.toString());
            calculateDr(feilds);
        }
    }

    public Long insertUserQuery() throws Exception {
        //   Insert to Table5
        long d = 0;
        currentUsersNumber++;
        int i = currentUsersNumber;
        String feilds = "users0id,users0firstname,users0lastname,"
                + " users0nickname, users0password,"
                + " users0email, users0rating, users0balance,"
                + " users0creation_date";
        StringBuilder builder5 = new StringBuilder();
        builder5.append("'Users0id").append(i).append("',");
        builder5.append("'Users0firstname").append(i).append("',");
        builder5.append("'Users0lastname").append(i).append("',");
        builder5.append("'Users0nickname").append(i).append("',");
        builder5.append("'Users0password").append(i).append("',");
        builder5.append("'Users0email").append(i).append("',");
        builder5.append("'0").append("',");
        builder5.append("'0").append("',");
        builder5.append("'Users0Creation_date").append(i).append("'");
        d += (insertRecord("table5", feilds, builder5.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterUser:table5", 1);

        // insert into stable 14
        feilds = "regions0id,users0id";
        StringBuilder builderTbl14 = new StringBuilder();
        builderTbl14.append("'regions0id").append(getRandomNumber(regionNumber)).append("',");
        builderTbl14.append("'users0id").append(i).append("'");
        d += (insertRecord("stable14", feilds, builderTbl14.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterUser:stable14", 1);
        return d;
    }

    public int getRandomNumber(int h) {
        Random r = new Random();
        int low = 0;
        int high = h + low;
        return r.nextInt(high - low) + low;
    }

    public Long UpdateQuery(String query) {
        long s = new Date().getTime();
//        System.out.println(query);
        ResultSet results = session.execute(query);
        long e = new Date().getTime();
        return e - s;
    }

    public long updateItem(Map<String, String> fields, int index) {
        long times = 0;
        String itemId = fields.get("items0id");

        // update: Table 3 
        // items0end_date=items0end_date655, items0max_bid=items0max_bid655, items0nb_of_bids=items0nb_of_bids655 
        String q = "update lightBidding.table3 set items0end_date='" + fields.get("items0end_date")
                + "',items0max_bid='" + fields.get("items0max_bid") + "', items0nb_of_bids='"
                + fields.get("items0nb_of_bids") + "' where items0id='" + itemId + "';";
        if (index == 1) {
            calculateAFM("StoreBuyNow:table3", 1);
        } else {
            calculateAFM("StoreBid:table3", 1);
        }
        times += (UpdateQuery(q));

        // table 4
        q = "select categories0id from lightBidding.stable15 where items0id = '" + itemId + "';";
        Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q);
        if (index == 1) {
            calculateAFM("StoreBuyNow:stable15", tuple3._3());

        } else {
            calculateAFM("StoreBid:stable15", tuple3._3());

        }
        times += (tuple3._1());
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            String cat0id = map.get("categories0id");
            map.remove("categories0id");
            String items0end_date = fields.get("items0end_date");
//            fields.remove("items0end_date");
            // items0quantity=items0quantity2431, items0nb_of_bids=items0nb_of_bids2431
            q = "update lightBidding.table4 set items0quantity='" + fields.get("items0quantity")
                    + "',items0nb_of_bids='" + fields.get("items0nb_of_bids")
                    + "' where categories0id='" + cat0id + "' AND items0end_date='" + items0end_date
                    + "' AND items0id='" + itemId + "';";
            times += (UpdateQuery(q));
            if (index == 1) {
                calculateAFM("StoreBuyNow:table4", 1);

            } else {
                calculateAFM("StoreBid:table4", 1);

            }
        }

        // table 9
        q = "select  buy_now0id, users0id, buy_now0date  from lightBidding.stable16 where items0id = '" + itemId + "';";
        tuple3 = execRecord(q);
        if (index == 1) {
            calculateAFM("StoreBuyNow:stable16", tuple3._3());

        } else {
            calculateAFM("StoreBid:stable16", tuple3._3());

        }
        times += (tuple3._1());
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
//
            //items0end_date=items0end_date2835, items0max_bid=items0max_bid2835, items0nb_of_bids=items0nb_of_bids2835
            q = "update lightBidding.table9 set items0end_date='" + fields.get("items0end_date") + "',"
                    + "items0max_bid='" + fields.get("items0max_bid") + "', items0nb_of_bids='" + fields.get("items0nb_of_bids")
                    + "' where users0id='" + map.get("users0id")
                    + "' AND buy_now0date='" + map.get("buy_now0date")
                    + "' AND items0id='" + itemId
                    + "' AND buy_now0id='" + map.get("buy_now0id") + "';";
            times += (UpdateQuery(q));
            if (index == 1) {
                calculateAFM("StoreBuyNow:table9", 1);

            } else {
                calculateAFM("StoreBid:table9", 1);

            }
        }
        String items0end_datetbl10 = null;
        // table 10
        q = "select users0id from lightBidding.stable19 where items0id = '" + itemId + "';";
        tuple3 = execRecord(q);
        if (index == 1) {
            calculateAFM("StoreBuyNow:stable19", tuple3._3());

        } else {
            calculateAFM("StoreBid:stable19", tuple3._3());

        }
        times += (tuple3._1());
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            items0end_datetbl10 = fields.get("items0end_date");
            q = "update lightBidding.table10 set  items0quantity='" + fields.get("items0quantity")
                    + "',items0nb_of_bids='" + fields.get("items0nb_of_bids")
                    + "' where users0id='" + map.get("users0id")
                    + "' AND items0end_date='" + items0end_datetbl10 + "' AND items0id='" + itemId + "';";
            times += (UpdateQuery(q));
            if (index == 1) {
                calculateAFM("StoreBuyNow:table10", 1);

            } else {
                calculateAFM("StoreBid:table10", 1);

            }
        }

        // table 11
//        itemId = entityIds.get("stable18:items").get(getRandomNumber(entityIds.get("stable18:items").size()));
        q = "select bids0id, users0id from lightBidding.stable18 where items0id = '" + itemId + "';";
        tuple3 = execRecord(q);
        if (index == 1) {
            calculateAFM("StoreBuyNow:stable18", tuple3._3());

        } else {
            calculateAFM("StoreBid:stable18", tuple3._3());

        }
        times += (tuple3._1());

        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            //
            q = "update lightBidding.table11 set items0quantity='" + fields.get("items0quantity")
                    + "',items0nb_of_bids='" + fields.get("items0nb_of_bids")
                    + "' where users0id='" + map.get("users0id")
                    + "' AND items0end_date='" + items0end_datetbl10
                    + "' AND items0id='" + itemId
                    + "' AND  bids0id='" + map.get("bids0id") + "';";
            times += (UpdateQuery(q));
            if (index == 1) {
                calculateAFM("StoreBuyNow:table11", 1);

            } else {
                calculateAFM("StoreBid:table11", 1);

            }
        }

        // table 12
        q = "select regions0id, categories0id, users0id from lightBidding.stable19 where items0id = '" + itemId + "';";
        tuple3 = execRecord(q);
        if (index == 1) {
            calculateAFM("StoreBuyNow:stable19", tuple3._3());

        } else {
            calculateAFM("StoreBid:stable19", tuple3._3());

        }
        times += (tuple3._1());
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            q = "update lightBidding.table12 set  items0quantity='" + fields.get("items0quantity")
                    + "',items0nb_of_bids='" + fields.get("items0nb_of_bids") + "' where items0id='" + itemId
                    + "' AND users0id='" + map.get("users0id")
                    + "' AND regions0id='" + map.get("regions0id")
                    + "' AND categories0id='" + map.get("categories0id")
                    + "' AND items0end_date='" + items0end_datetbl10 + "';";
            times += (UpdateQuery(q));
            if (index == 1) {
                calculateAFM("StoreBuyNow:table12", 1);

            } else {
                calculateAFM("StoreBid:table12", 1);

            }
        }

        if (index == 1) {
            // table 15
            q = "select categories0id from lightBidding.stable15 where items0id = '" + itemId + "';";
            tuple3 = execRecord(q);
            if (index == 1) {
                calculateAFM("StoreBuyNow:stable15", tuple3._3());

            } else {
                calculateAFM("StoreBid:stable15", tuple3._3());

            }
            times += (tuple3._1());
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                q = "update lightBidding.stable15 set items0end_date='" + items0end_datetbl10 + "' where items0id='" + itemId
                        + "' AND categories0id='" + map.get("categories0id") + "';";
                times += (UpdateQuery(q));
                if (index == 1) {
                    calculateAFM("StoreBuyNow:stable15", 1);

                } else {
                    calculateAFM("StoreBid:stable15", 1);

                }
            }

            // table 18
//            itemId = entityIds.get("stable18:items").get(getRandomNumber(entityIds.get("stable18:items").size()));
            q = "select users0id, bids0id from lightBidding.stable18 where items0id = '" + itemId + "';";
            tuple3 = execRecord(q);
            if (index == 1) {
                calculateAFM("StoreBuyNow:stable18", tuple3._3());

            } else {
                calculateAFM("StoreBid:stable18", tuple3._3());

            }
            times += (tuple3._1());
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                //bids0id=bids0id6284 AND  users0id=users0id7690
                q = "update lightBidding.stable18 set items0end_date='" + items0end_datetbl10 + "' where items0id='" + itemId
                        + "' AND bids0id='" + map.get("bids0id") + "' AND users0id='" + map.get("users0id") + "';";
                times += (UpdateQuery(q));
                if (index == 1) {
                    calculateAFM("StoreBuyNow:stable18", 1);

                } else {
                    calculateAFM("StoreBid:stable18", 1);

                }
            }

            // table 19
            q = "select regions0id, categories0id, users0id from lightBidding.stable19 where items0id = '" + itemId + "';";
            tuple3 = execRecord(q);
            if (index == 1) {
                calculateAFM("StoreBuyNow:stable19", tuple3._3());

            } else {
                calculateAFM("StoreBid:stable19", tuple3._3());

            }
            times += (tuple3._1());
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                //regions0id=regions0idnull AND  categories0id=categories0id16 AND  users0id=users0id9759
                q = "update lightBidding.stable19 set items0end_date='" + items0end_datetbl10 + "' where items0id='" + itemId
                        + "' AND regions0id='" + map.get("regions0id")
                        + "' AND categories0id='" + map.get("categories0id")
                        + "' AND users0id='" + map.get("users0id") + "';";
                times += (UpdateQuery(q));
                if (index == 1) {
                    calculateAFM("StoreBuyNow:stable19", 1);

                } else {
                    calculateAFM("StoreBid:stable19", 1);
                }
            }
        }

        return times;
    }

    public Map<String, String> getRecord(String tblName, String field, String value) {
        Map<String, String> map = new HashMap<>();
        String q = "SELECT * FROM lightBidding." + tblName + " WHERE  " + field + " = '" + value + "';";
//        System.out.println(q);
        ResultSet results = session.execute(q);
        List<ColumnDefinitions.Definition> definitions = results.getColumnDefinitions().asList();
        for (Row row : results) {
            for (ColumnDefinitions.Definition definition : definitions) {
                map.put(definition.getName(), row.getString(definition.getName()));
            }
        }
        return map;
    }

    public Tuple3<Long, List<Row>, Integer> execRecord(String query) {
        List<Row> rows = new ArrayList<>();
        long s = new Date().getTime();
//        System.out.println(">>>>>>>>>>>>>>>>>" + query + "<<<<<<<<<<<<<<<<<<<");
        ResultSet results = session.execute(query);
        int count = 0;
        for (Row row : results) {
            count++;
            rows.add(row);
        }
//        printResultset(rows);
        long e = new Date().getTime();
        return new Tuple3<>(e - s, rows, count);
    }

    public void printResultset(List<Row> results) {
        for (Row row : results) {
            List<ColumnDefinitions.Definition> definitions = row.getColumnDefinitions().asList();
            for (ColumnDefinitions.Definition definition : definitions) {
                System.out.println(definition.getName() + " : " + row.getString(definition.getName()));
            }
        }
    }

    public List<String> execEntityIds(String tblName, String field) {
        List<String> list = new ArrayList<>();
        ResultSet results = session.execute("select " + field + " from " + tblName);
        List<ColumnDefinitions.Definition> definitions = results.getColumnDefinitions().asList();
        ColumnDefinitions.Definition definition = definitions.get(0);
        for (Row row : results) {
            list.add(row.getString(definition.getName()));
        }

        return list;
    }

    public double readBrowseCategories(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname, users0password from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select categories0id, categories0name from lightBidding.table1 where categories0dummy = '0'";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "';");
            calculateAFM("BrowseCategories:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2);
            calculateAFM("BrowseCategories:table1", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewBidHistory(int number, BufferedWriter writer) throws Exception {
        String q1 = "select items0name from lightBidding.table3 where items0id = 'items0id";
        String q2 = "select users0id, users0nickname, bids0id, bids0qty, bids0bid, bids0date from lightBidding.table2 where items0id = 'items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "';");
            calculateAFM("ViewBidHistory:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "';");
            calculateAFM("ViewBidHistory:table2", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewItem(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from lightBidding.table3 where items0id = 'items0id";
        String q2 = "select * from lightBidding.table2 where items0id = 'items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "';");
            calculateAFM("ViewItem:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "';");
            calculateAFM("ViewItem:table2", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readSearchItemsByCategory(int number, BufferedWriter writer) throws Exception {
        String q1;
        double sum = 0;
        for (int i = 0; i < number; i++) {
            int items0id = getRandomNumber(itemsNumber);
            q1 = "select * from lightBidding.table4 where categories0id ='categories0id" + String.valueOf(getRandomNumber(categoryNumber)) + "' AND items0end_date> 'items0end_date" + String.valueOf(items0id) + "';";
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1);
            calculateAFM("SearchItemsByCategory:table4", tuple3._3());
            sum += tuple3._1();
            writer.write(String.valueOf(tuple3._1()));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewUserInfo(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select comments0id, comments0rating, comments0date, comments0comment from lightBidding.table6 where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "';");
            calculateAFM("ViewUserInfo:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + userId + "';");
            calculateAFM("ViewUserInfo:table6", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readRegisterItem(int number, BufferedWriter writer) throws Exception {
        double sum = 0;
        long l = insertItemQuery();
        writer.write(String.valueOf(l));
        writer.write("\n");
        sum += l;
        return sum;
    }

    public double readRegisterUser(int number, BufferedWriter writer) throws Exception {
        double sum = 0;
        long l = insertUserQuery();
        writer.write(String.valueOf(l));
        writer.write("\n");
        sum += l;
        return sum;
    }

    public double readBuyNow(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select * from lightBidding.table3 where items0id = 'items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "';");
            calculateAFM("BuyNow:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "';");
            calculateAFM("BuyNow:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreBuyNow(int number, BufferedWriter writer) throws Exception {
        String q1 = "select items0quantity, items0nb_of_bids, items0max_bid, items0end_date from lightBidding.table3 where items0id ='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String items0id = "items0id" + String.valueOf(getRandomNumber(itemsNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + items0id + "';");
            calculateAFM("StoreBuyNow:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                map.put("items0id", items0id);
                long l = updateItem(map, 1);
                d += l;
                sum += l;
                l = insertBuyNowQuery(items0id);
                d += l;
                sum += l;
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readPutBid(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select * from lightBidding.table3 where items0id = 'items0id";
        String q3 = "select bids0qty, bids0date from lightBidding.mv_table2 where items0id ='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "';");
            calculateAFM("PutBid:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "';");
            calculateAFM("PutBid:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3 + row.getString(0) + "';");
                calculateAFM("PutBid:mView", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }

            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreBid(int number, BufferedWriter writer) throws Exception {
        Map<String, String> map = new HashMap<>();
        double sum = 0;
        long d = 0;
        String items0id = "items0id" + String.valueOf(getRandomNumber(itemsNumber));
        map.put("items0id", items0id);
        long l = insertBidsQuery(map);
        d += l;
        sum += l;
        String q1 = "select items0nb_of_bids, items0max_bid,items0end_date from lightBidding.table3 where items0id ='";
        for (int i = 0; i < number; i++) {
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + items0id + "';");
            calculateAFM("StoreBid:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                l = updateItem(map, 0);
                d += l;
                sum += l;
            }

            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readPutComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname, users0password from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select * from lightBidding.table3 where items0id ='items0id";
        String q3 = "select * from lightBidding.table5 where users0id ='Users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "';");
            calculateAFM("PutComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "';");
            calculateAFM("PutComment:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + String.valueOf(getRandomNumber(usersNumber)) + "';");
            calculateAFM("PutComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0rating from lightBidding.table5 where users0id ='Users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String user0id = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + user0id + "';");
            calculateAFM("StoreComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                String q = "update lightBidding.table5 set users0rating='" + map.get("users0rating") + "' where users0id='users0id" + user0id + "';";
                long l = UpdateQuery(q);
                calculateAFM("StoreComment:table5", 1);
                d += l;
                sum += l;
                map.put("users0id", user0id);
                l = insertCommentsQuery(map);
                d += l;
                sum += l;
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readAboutMe(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from lightBidding.table5 where users0id ='Users0id";
        String q2 = "select * from lightBidding.table6 where users0id ='Users0id";
        String q3 = "select users0nickname from lightBidding.table8 where comments0id ='comments0id";
        String q4 = "select * from lightBidding.table9 where users0id='users0id";
        String q5 = "select * from lightBidding.table10 where users0id ='users0id";
        String q6 = "select * from lightBidding.table11 where users0id = 'users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            String buyId = String.valueOf(getRandomNumber(buysNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "';");
            calculateAFM("AboutMe:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + userId + "';");
            calculateAFM("AboutMe:table6", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + String.valueOf(getRandomNumber(commentsNumber)) + "';");
            calculateAFM("AboutMe:table8", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q4 + userId + "' and buy_now0date >='buy_now0date" + buyId + "';");
            calculateAFM("AboutMe:table9", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();

            tuple3 = execRecord(q5 + userId + "' and items0end_date>= 'items0end_date" + itemId + "';");
            calculateAFM("AboutMe:table10", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q6 + userId + "';");
            calculateAFM("AboutMe:table11", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readSearchItemsByRegion(int number, BufferedWriter writer) throws Exception {
        double sum = 0;
        for (int i = 0; i < number; i++) {
            String reg_cat = entityIds.get("table12").get(getRandomNumber(entityIds.get("table12").size()));
            String regId = reg_cat.split(":")[0];
            String catId = reg_cat.split(":")[1];
            String str = "select * from lightBidding.table12 where regions0id='" + regId
                    + "' AND categories0id = '" + catId + "';";// "' AND items0end_date>'items0end_date"
            //+ String.valueOf(getRandomNumber(itemsNumber)) + 
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(str);
            calculateAFM("SearchItemsByRegion:table12", tuple3._3());
            sum += tuple3._1();
            writer.write(String.valueOf(tuple3._1()));
            writer.write("\n");
        }
        return sum;
    }

    public double readBrowseRegions(int number, BufferedWriter writer) throws Exception {
        String q1 = "select regions0id, regions0name from lightBidding.table13 where regions0dummy = '0'";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1);
            calculateAFM("BrowseRegions:table13", tuple3._3());
            sum += tuple3._1();
            writer.write(String.valueOf(tuple3._1()));
            writer.write("\n");
        }
        return sum;
    }

    public void writeAfmAndDrToFile(String baseAdd) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(baseAdd + "/afm.txt"));
        writer.write("AFM Result:\n");
//        for (String string : afm.keySet()) {
//            writer.write(string + " : " + afm.get(string) + "\n");
//        }
        SortedSet<String> keys = new TreeSet<>(afm.keySet());
        for (String key : keys) {
            writer.write(key + " : " + afm.get(key) + "\n\n");
        }
        long sum = afm.values().stream().reduce(new BinaryOperator<Long>() {
            @Override
            public Long apply(Long t, Long u) {
                return t + u;
            }
        }).get();
        writer.write("======== SUM ======\n");
        writer.write("Sum : " + sum);
        writer.close();
        writer = new BufferedWriter(new FileWriter(baseAdd + "/dr.txt"));
        writer.write("DR Result:\n");
        for (String string : dr.keySet()) {
            writer.write(string + " : " + dr.get(string) + "\n");
        }
        int s = dr.values().stream().reduce(new BinaryOperator<Integer>() {
            @Override
            public Integer apply(Integer t, Integer u) {
                return t + u;
            }
        }).get();
        writer.write("======== SUM ======\n");
        writer.write("Sum : " + s);
        writer.close();
    }

    public void calculateDr(String query) {
        for (String field : query.replaceAll("text", "").split(",")) {
            if (dr.containsKey(field.toLowerCase().trim())) {
                dr.put(field.toLowerCase().trim(), dr.get(field.toLowerCase().trim()) + 1);
            } else {
                dr.put(field.toLowerCase().trim(), 1);
            }
        }
    }

    public void calculateAFM(String tmp, Integer count) {
        String transactionTableName = tmp.split(":")[1] + ":" + tmp.split(":")[0];
        if (afm.containsKey(transactionTableName.toLowerCase().trim())) {
            afm.put(transactionTableName.toLowerCase().trim(), afm.get(transactionTableName.toLowerCase().trim()) + count);
        } else {
            afm.put(transactionTableName.toLowerCase().trim(), 1l);
        }
    }

}
