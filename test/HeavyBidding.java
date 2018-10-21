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

public class HeavyBidding {

    private Cluster cluster;
    private Session session;
    private String regionAddress;
    private String categoryAddress;
    private Map<String, List<String>> entityIds;
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

    public HeavyBidding() {
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
        registerItem = 46200;
        registerUser = 92900;
        buyNow = 1005;
        storeBuyNow = 95600;
        putBid = 4670;
        storeBid = 323700;
        putComment = 402;
        storeComment = 38900;
        aboutMe = 1477;
        searchItemsByRegion = 5489;
        browseRegions = 1963;
    }

    public static void main(String[] args) throws Exception {

        String heavyBiddingBaseAdd = "/home/user/heavyBidding";
        HeavyBidding mainHeavyBidding = new HeavyBidding();
        mainHeavyBidding.connect("192.168.82.214");
        mainHeavyBidding.getSession();
        mainHeavyBidding.createSchema("heavyBidding");
        mainHeavyBidding.createSchemaAndInsertData();
        mainHeavyBidding.randomRun(heavyBiddingBaseAdd);
        mainHeavyBidding.writeAfmAndDrToFile(heavyBiddingBaseAdd);
        mainHeavyBidding.close();
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
            if (countMap.get(st) % 700 == 0) {
                System.out.println("Heavy Bidding:" + st + ": " + countMap.get(st));
            }
            if (countMap.get(st) == 0) {
                countMap.remove(st);
                writerMap.get(st)._1().write("======================================\nAvg Time: ");
                double avg = sumMap.get(st) / writerMap.get(st)._2();
                writerMap.get(st)._1().write(String.valueOf(avg));
                writerMap.get(st)._1().close();
                System.out.println(st + " Finished!");
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
        createSchema("heavyBidding");
        loadUserQueryToCassandra(usersNumber);
        System.out.println("loadUserQueryToCassandra");
        loadCommentsQueryToCassandra(commentsNumber);
        System.out.println("loadCommentsQueryToCassandra");
        loadItemQueryToCassandra(itemsNumber);
        System.out.println("loadItemQueryToCassandra");
        loadBuyNowQueryToCassandra(buysNumber);
        System.out.println("loadBuyNowQueryToCassandra");
        loadBidsQueryToCassandra(bidsNumber);
        System.out.println("loadBidsQueryToCassandra");
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
        String q = "SELECT regions0id, categories0id FROM heavyBidding.table12;";
        ResultSet results = session.execute(q);
        for (Row row : results) {
            entityIds.get("table12").add(row.getString(0) + ":" + row.getString(1));
        }

        String q1 = "SELECT users0id FROM heavyBidding.table5;";
        ResultSet results1 = session.execute(q1);
        currentUsersNumber = -1;
        for (Row row : results1) {
            if (currentUsersNumber < Integer.valueOf(row.getString(0).toLowerCase().replaceAll("users0id", ""))) {
                currentUsersNumber = Integer.valueOf(row.getString(0).toLowerCase().replaceAll("users0id", ""));
            }
        }
        q1 = "SELECT items0id FROM heavyBidding.table3;";
        results1 = session.execute(q1);
        currentItemsNumber = -1;
        for (Row row : results1) {
            if (currentItemsNumber < Integer.valueOf(row.getString(0).replaceAll("items0id", ""))) {
                currentItemsNumber = Integer.valueOf(row.getString(0).replaceAll("items0id", ""));
            }
        }

        q1 = "SELECT bids0id FROM heavyBidding.table22;";
        results1 = session.execute(q1);
        currentBidsNumber = -1;
        for (Row row : results1) {
            if (currentBidsNumber < Integer.valueOf(row.getString(0).replaceAll("bids0id", ""))) {
                currentBidsNumber = Integer.valueOf(row.getString(0).replaceAll("bids0id", ""));
            }
        }
        q1 = "SELECT comments0id FROM heavyBidding.table8;";
        results1 = session.execute(q1);
        currentCommentNumber = -1;
        for (Row row : results1) {
            if (currentCommentNumber < Integer.valueOf(row.getString(0).replaceAll("comments0id", ""))) {
                currentCommentNumber = Integer.valueOf(row.getString(0).replaceAll("comments0id", ""));
            }
        }
        q1 = "SELECT buy_now0id FROM heavyBidding.table92;";
        results1 = session.execute(q1);
        currentBuyNumber = -1;
        for (Row row : results1) {
            if (currentBuyNumber < Integer.valueOf(row.getString(0).replaceAll("buy_now0id", ""))) {
                currentBuyNumber = Integer.valueOf(row.getString(0).replaceAll("buy_now0id", ""));
            }
        }
    }

    public void createSchema(String ksName) throws Exception {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use " + ksName);
        session.execute("CREATE TABLE  IF NOT EXISTS table1 (categories0dummy text,categories0id text,categories0name text, primary KEY (categories0dummy,categories0id));");
        session.execute("CREATE TABLE  IF NOT EXISTS table2 (items0id text, bids0id text,  users0id text,users0nickname text, items0end_date text, primary KEY (items0id, bids0id, users0id));");
        session.execute("CREATE TABLE  IF NOT EXISTS table22 (bids0id text,  bids0date text,bids0bid text, bids0qty text, primary KEY (bids0id, bids0date));");
        session.execute("CREATE TABLE IF NOT EXISTS  table3 (items0id text, items0end_date text, items0name text, items0max_bid text, items0quantity text,  items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text,items0buy_now text, primary KEY (items0id, items0end_date)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS table4 (categories0id text,items0id text, primary KEY (categories0id, items0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table5 (users0id text,users0email text, users0nickname text, users0lastname text, users0rating text, users0creation_date text, users0firstname text, users0password text, users0balance text, primary KEY (users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table6 (users0id text, comments0id text, comments0comment text, comments0date text, comments0rating text, primary KEY (users0id,comments0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table7 (users0id text, comments0id text, comments0comment text, comments0date text, comments0rating text, primary KEY (users0id,comments0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS table8 (comments0id text, users0id text, users0nickname text, primary KEY (comments0id,users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table9 (users0id text, items0id text, buy_now0id text, primary KEY (users0id, items0id, buy_now0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table92 (buy_now0id text, buy_now0date text, buy_now0qty text, primary KEY (buy_now0id, buy_now0date));");
        session.execute("CREATE TABLE IF NOT EXISTS table10 (users0id text,items0id text, primary KEY (users0id, items0id));");
        session.execute("CREATE TABLE IF NOT EXISTS table11 (users0id text,items0id text, bids0id text, primary KEY (users0id, items0id, bids0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  table12 (regions0id text, categories0id text,items0id text, users0id text, primary KEY (regions0id, categories0id, items0id, users0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  table13 (regions0dummy text,regions0id text,regions0name text, primary KEY (regions0dummy,regions0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  stable14 (users0id text,regions0id text, primary KEY (users0id,regions0id)) ;");
        session.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_table22 AS select bids0id, bids0date, bids0bid, bids0qty from table22 WHERE bids0id IS NOT NULL AND bids0date IS NOT NULL AND bids0bid IS NOT NULL PRIMARY KEY(bids0id, bids0date, bids0bid);");

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
//        String q = "INSERT INTO heavyBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");";
//        System.out.println("INSERT INTO heavyBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        session.execute("INSERT INTO heavyBidding." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        long e = new Date().getTime();
        return e - s;
    }

    public void loadItemQueryToCassandra(int itemCount) throws Exception {
        for (int i = 0; i < itemCount; i++) {
            int userRand = getRandomNumber(usersNumber);
            int catRand = getRandomNumber(categoryNumber);
            // insert into table 3
            String feilds = "items0id, items0end_date, items0name, items0max_bid, items0quantity,  items0reserve_price, items0nb_of_bids, items0start_date, items0description, items0initial_price, items0buy_now";
            StringBuilder builder = new StringBuilder();
            builder.append("'items0id").append(i).append("',");
            builder.append("'items0end_date").append(i).append("',");
            builder.append("'items0name").append(i).append("',");
            builder.append("'items0max_bid").append(i).append("',");
            builder.append("'items0quantity").append(i).append("',");
            builder.append("'items0reserve_price").append(i).append("',");
            builder.append("'items0nb_of_bids").append(i).append("',");
            builder.append("'items0start_date").append(i).append("',");
            builder.append("'items0description").append(i).append("',");
            builder.append("'items0initial_price").append(i).append("',");
            builder.append("'items0buy_now").append(i).append("'");
            insertRecord("table3", feilds, builder.toString());
            // insert into table 4
            feilds = "categories0id ,items0id";
            StringBuilder builderTbl4 = new StringBuilder();
            builderTbl4.append("'categories0id").append(catRand).append("',");
            builderTbl4.append("'items0id").append(i).append("'");
            insertRecord("table4", feilds, builderTbl4.toString());
            // insert into table 10
            feilds = "users0id, items0id";
            StringBuilder builderTbl10 = new StringBuilder();
            builderTbl10.append("'users0id").append(userRand).append("',");
            builderTbl10.append("'items0id").append(i).append("'");
            insertRecord("table10", feilds, builderTbl10.toString());
            // insert into table 12
            feilds = "regions0id, categories0id ,items0id, users0id";
            StringBuilder builderTbl12 = new StringBuilder();
            String regions0id = getRecord("stable14", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");
            builderTbl12.append("'").append(regions0id).append("',");
            builderTbl12.append("'categories0id").append(catRand).append("',");
            builderTbl12.append("'items0id").append(i).append("',");
            builderTbl12.append("'users0id").append(userRand).append("'");
            insertRecord("table12", feilds, builderTbl12.toString());
        }
    }

    public Long insertItemQuery() throws Exception {
        long d = 0;
        currentItemsNumber++;
        int i = currentItemsNumber;
        int userRand = getRandomNumber(usersNumber);
        int catRand = getRandomNumber(categoryNumber);
        // insert into table 3
        String feilds = "items0id, items0end_date, items0name, items0max_bid, items0quantity,  items0reserve_price, items0nb_of_bids, items0start_date, items0description, items0initial_price, items0buy_now";
        StringBuilder builder = new StringBuilder();
        builder.append("'items0id").append(i).append("',");
        builder.append("'items0end_date").append(i).append("',");
        builder.append("'items0name").append(i).append("',");
        builder.append("'items0max_bid").append(i).append("',");
        builder.append("'items0quantity").append(i).append("',");
        builder.append("'items0reserve_price").append(i).append("',");
        builder.append("'items0nb_of_bids").append(i).append("',");
        builder.append("'items0start_date").append(i).append("',");
        builder.append("'items0description").append(i).append("',");
        builder.append("'items0initial_price").append(i).append("',");
        builder.append("'items0buy_now").append(i).append("'");
        d += (insertRecord("table3", feilds, builder.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table3", 1);
        // insert into table 4
        feilds = "categories0id ,items0id";
        StringBuilder builderTbl4 = new StringBuilder();
        builderTbl4.append("'categories0id").append(catRand).append("',");
        builderTbl4.append("'items0id").append(i).append("'");
        d += (insertRecord("table4", feilds, builderTbl4.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table4", 1);

        // insert into table 10
        feilds = "users0id, items0id";
        StringBuilder builderTbl10 = new StringBuilder();
        builderTbl10.append("'users0id").append(userRand).append("',");
        builderTbl10.append("'items0id").append(i).append("'");
        d += (insertRecord("table10", feilds, builderTbl10.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table10", 1);

        // insert into table 12
        feilds = "regions0id, categories0id ,items0id, users0id";
        StringBuilder builderTbl12 = new StringBuilder();
        String regions0id = getRecord("stable14", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");
        builderTbl12.append("'").append(regions0id).append("',");
        builderTbl12.append("'categories0id").append(catRand).append("',");
        builderTbl12.append("'items0id").append(i).append("',");
        builderTbl12.append("'users0id").append(userRand).append("'");
        d += (insertRecord("table12", feilds, builderTbl12.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:table12", 1);
        return d;
    }

    public void loadBuyNowQueryToCassandra(int buyNumber) throws Exception {
        for (int i = 0; i < buyNumber; i++) {
            // insert into table 9
            int userRand = getRandomNumber(usersNumber);
            int itemRand = getRandomNumber(itemsNumber);
            String feilds = "users0id,items0id, buy_now0id";
            StringBuilder builderTbl9 = new StringBuilder();
            builderTbl9.append("'users0id").append(userRand).append("',");
            builderTbl9.append("'items0id").append(itemRand).append("',");
            builderTbl9.append("'buy_now0id").append(i).append("'");
            insertRecord("table9", feilds, builderTbl9.toString());

            // insert into table 92
            feilds = "buy_now0id,buy_now0date, buy_now0qty";
            StringBuilder builderTbl92 = new StringBuilder();
            builderTbl92.append("'buy_now0id").append(i).append("',");
            builderTbl92.append("'buy_now0date").append(i).append("',");
            builderTbl92.append("'buy_now0qty").append(i).append("'");
            insertRecord("table92", feilds, builderTbl92.toString());
        }
    }

    public long insertBuyNowQuery(String itemId) throws Exception {
        long times = 0;
        // insert into table 9
        currentBuyNumber++;
        int buyRand = currentBuyNumber;
        int userRand = getRandomNumber(usersNumber);
        String feilds = "users0id,items0id, buy_now0id";
        StringBuilder builderTbl9 = new StringBuilder();
        builderTbl9.append("'users0id").append(userRand).append("',");
        builderTbl9.append("'items0id").append(itemId).append("',");
        builderTbl9.append("'buy_now0id").append(buyRand).append("'");
        times += (insertRecord("table9", feilds, builderTbl9.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBuyNow:table9", 1);

        // insert into table 92
        feilds = "buy_now0id, buy_now0date, buy_now0qty";
        StringBuilder builderTbl92 = new StringBuilder();
        builderTbl92.append("'buy_now0id").append(buyRand).append("',");
        builderTbl92.append("'buy_now0date").append(buyRand).append("',");
        builderTbl92.append("'buy_now0qty").append(buyRand).append("'");
        times += (insertRecord("table92", feilds, builderTbl92.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBuyNow:table92", 1);
        return times;
    }

    public void loadBidsQueryToCassandra(int bidNumber) throws Exception {
        // insert into table 2
        for (int i = 0; i < bidNumber; i++) {
            int itemRand = getRandomNumber(itemsNumber);
            int userRand = getRandomNumber(usersNumber);
            String feilds = "items0id, bids0id, users0id, users0nickname, items0end_date";
            String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(userRand)).get("users0nickname");
            StringBuilder builderTbl2 = new StringBuilder();
            builderTbl2.append("'items0id").append(itemRand).append("',");
            builderTbl2.append("'bids0id").append(i).append("',");
            builderTbl2.append("'users0id").append(userRand).append("',");
            builderTbl2.append("'").append(users0nickname).append("',");
            builderTbl2.append("'items0end_date").append(itemRand).append("'");
            insertRecord("table2", feilds, builderTbl2.toString());

            // insert into table 22
            feilds = "bids0id, bids0date, bids0bid, bids0qty";
            StringBuilder builderTbl22 = new StringBuilder();
            builderTbl22.append("'bids0id").append(i).append("',");
            builderTbl22.append("'bids0date").append(i).append("',");
            builderTbl22.append("'bids0bid").append(i).append("',");
            builderTbl22.append("'bids0qty").append(i).append("'");
            insertRecord("table22", feilds, builderTbl22.toString());

            // insert into table 11
            feilds = "users0id, items0id, bids0id";
            StringBuilder builderTbl11 = new StringBuilder();
            builderTbl11.append("'users0id").append(userRand).append("',");
            builderTbl11.append("'items0id").append(itemRand).append("',");
            builderTbl11.append("'bids0id").append(i).append("'");
            insertRecord("table11", feilds, builderTbl11.toString());
        }
    }

    public Long insertBidsQuery(Map<String, String> map) throws Exception {
        long times = 0;
        // insert into table 2
        currentBidsNumber++;
        int bidRand = currentBidsNumber;
        int userRand = getRandomNumber(usersNumber);
        String feilds = "items0id, bids0id, users0id,users0nickname, items0end_date";
        String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(userRand)).get("users0nickname");
        StringBuilder builderTbl2 = new StringBuilder();
        builderTbl2.append("'").append(map.get("items0id")).append("',");
        builderTbl2.append("'bids0id").append(bidRand).append("',");
        builderTbl2.append("'users0id").append(userRand).append("',");
        builderTbl2.append("'").append(users0nickname).append("',");
        builderTbl2.append("'").append(map.get("items0id").replaceAll("id", "end_date")).append("'");
        times += (insertRecord("table2", feilds, builderTbl2.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBid:table2", 1);

        // insert into Table22
        feilds = "bids0id, bids0date, bids0bid, bids0qty";
        StringBuilder builderTbl22 = new StringBuilder();
        builderTbl22.append("'bids0id").append(bidRand).append("',");
        builderTbl22.append("'bids0date").append(bidRand).append("',");
        builderTbl22.append("'bids0bid").append(bidRand).append("',");
        builderTbl22.append("'bids0qty").append(bidRand).append("'");
        times += (insertRecord("table22", feilds, builderTbl22.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBid:table22", 1);

        // insert into table 11
        feilds = "users0id, items0id, bids0id";
        StringBuilder builderTbl11 = new StringBuilder();
        builderTbl11.append("'users0id").append(userRand).append("',");
        builderTbl11.append("'").append(map.get("items0id")).append("',");
        builderTbl11.append("'bids0id").append(bidRand).append("'");
        times += (insertRecord("table11", feilds, builderTbl11.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBid:table11", 1);

        return times;
    }

    public void loadCommentsQueryToCassandra(int commentCount) throws Exception {
        // insert into table 6
        for (int i = 0; i < commentCount; i++) {
            int userRand = getRandomNumber(usersNumber);

            String feilds = "users0id, comments0id, comments0comment, comments0date, comments0rating";
            StringBuilder builderTbl6 = new StringBuilder();
            builderTbl6.append("'users0id").append(userRand).append("',");
            builderTbl6.append("'comments0id").append(i).append("',");
            builderTbl6.append("'comments0comment").append(i).append("',");
            builderTbl6.append("'comments0date").append(i).append("',");
            builderTbl6.append("'comments0rating").append(i).append("'");
            insertRecord("table6", feilds, builderTbl6.toString());

            // insert into table 7
            feilds = "users0id, comments0id, comments0comment, comments0date, comments0rating";
            StringBuilder builderTbl7 = new StringBuilder();
            builderTbl7.append("'users0id").append(userRand).append("',");
            builderTbl7.append("'comments0id").append(i).append("',");
            builderTbl7.append("'comments0comment").append(i).append("',");
            builderTbl7.append("'comments0date").append(i).append("',");
            builderTbl7.append("'comments0rating").append(i).append("'");
            insertRecord("table7", feilds, builderTbl7.toString());

            // insert into table 8
            feilds = " comments0id, users0id, users0nickname";
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
        String users0nickname = getRecord("table5", "users0id", "users0id" + String.valueOf(map.get("users0id"))).get("users0nickname");
        StringBuilder builderTbl8 = new StringBuilder();
        builderTbl8.append("'comments0id").append(commentRand).append("',");
        builderTbl8.append("'users0id").append(map.get("users0id")).append("',");
        builderTbl8.append("'").append(users0nickname).append("'");
        times += (insertRecord("table8", feilds, builderTbl8.toString()));
        calculateDr(feilds);
        calculateAFM("StoreComment:table8", 1);
        return times;
    }

    public void loadUserQueryToCassandra(int userCount) throws Exception {
        //   Insert to Table5
        for (int i = 0; i < userCount; i++) {
            String feilds = "users0id, users0email, users0nickname, users0lastname, users0rating, users0creation_date, users0firstname, users0password, users0balance";
            StringBuilder builder5 = new StringBuilder();
            builder5.append("'users0id").append(i).append("',");
            builder5.append("'users0email").append(i).append("',");
            builder5.append("'users0nickname").append(i).append("',");
            builder5.append("'users0lastname").append(i).append("',");
            builder5.append("'users0rating").append(i).append("',");
            builder5.append("'users0creation_date").append(i).append("',");
            builder5.append("'users0firstname").append(i).append("',");
            builder5.append("'users0password").append(i).append("',");
            builder5.append("'users0balance").append(i).append("'");
            insertRecord("table5", feilds, builder5.toString());

            // insert into stable 14
            feilds = "regions0id,users0id";
            calculateDr(feilds);
            StringBuilder builderTbl14 = new StringBuilder();
            builderTbl14.append("'regions0id").append(getRandomNumber(regionNumber)).append("',");
            builderTbl14.append("'users0id").append(i).append("'");
            insertRecord("stable14", feilds, builderTbl14.toString());
        }
    }

    public Long insertUserQuery() throws Exception {
        currentUsersNumber++;
        int i = currentUsersNumber;
        //   Insert to Table5
        long d = 0;
        String feilds = "users0id, users0email, users0nickname, users0lastname, users0rating, users0creation_date, users0firstname, users0password, users0balance";
        StringBuilder builder5 = new StringBuilder();
        builder5.append("'users0id").append(i).append("',");
        builder5.append("'users0email").append(i).append("',");
        builder5.append("'users0nickname").append(i).append("',");
        builder5.append("'users0lastname").append(i).append("',");
        builder5.append("'users0rating").append(i).append("',");
        builder5.append("'users0creation_date").append(i).append("',");
        builder5.append("'users0firstname").append(i).append("',");
        builder5.append("'users0password").append(i).append("',");
        builder5.append("'users0balance").append(i).append("'");
        d += (insertRecord("table5", feilds, builder5.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterUser:table5", 1);

        // insert into stable 14
        feilds = "regions0id,users0id";
        calculateDr(feilds);
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
//        System.out.println("UPDATE::::::::::::>>>>>>>>>>>>>>>>>" + query + "<<<<<<<<<<<<<<<<<");
        ResultSet results = session.execute(query);
        long e = new Date().getTime();
        return e - s;
    }

    public Long updateItem(Map<String, String> fields) {
        long times = 0;
        String items0id = fields.get("items0id");
        String items0end_date = fields.get("items0id").replaceAll("id", "end_date");
        // update: Table 3 
        String tmp = "items0nb_of_bids='" + fields.get("items0nb_of_bids") + "',items0max_bid='" + fields.get("items0max_bid") + "'";
        String q = "update heavyBidding.table3 set " + tmp + " where items0id='" + items0id + "' AND items0end_date='" + items0end_date + "';";
        times += (UpdateQuery(q));
        calculateAFM("StoreBid:table3", 1);
        return times;
    }

    public Map<String, String> getRecord(String tblName, String field, String value) {
        Map<String, String> map = new HashMap<>();
        String q = "SELECT * FROM heavyBidding." + tblName + " WHERE  " + field + " = '" + value + "';";
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
        String q1 = "select users0nickname, users0password from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select categories0id, categories0name from heavyBidding.table1 where categories0dummy = '0'";
        double sum = 0;

        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
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
        String q1 = "select items0name from heavyBidding.table3 where items0id = 'items0id";
        String q2 = "select users0id, users0nickname, bids0id from heavyBidding.table2 where items0id = 'items0id";
        String q3 = "select bids0qty, bids0bid, bids0date from heavyBidding.table22 where bids0id = ";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "';");
            calculateAFM("ViewBidHistory:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "'");
            calculateAFM("ViewBidHistory:table2", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3 + "'" + row.getString("bids0id") + "'");
                calculateAFM("ViewBidHistory:table22", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewItem(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from heavyBidding.table3 where items0id = 'items0id";
        String q2 = "select bids0id from heavyBidding.table2 where items0id = 'items0id";
        String q3 = "select bids0qty, bids0bid, bids0date from heavyBidding.table22 where bids0id= ";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "'");
            calculateAFM("ViewItem:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "'");
            calculateAFM("ViewItem:table2", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3 + "'" + row.getString(0) + "'");
                calculateAFM("ViewItem:table22", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readSearchItemsByCategory(int number, BufferedWriter writer) throws Exception {
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String categoryId = String.valueOf(getRandomNumber(categoryNumber));
            String q1 = "select items0id from heavyBidding.table4 where categories0id ='categories0id" + categoryId + "';";
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1);
            calculateAFM("SearchItemsByCategory:table4", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String q3 = "select * from heavyBidding.table3 where items0id='" + row.getString(0) + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3);
                calculateAFM("SearchItemsByCategory:table3", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewUserInfo(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select comments0id, comments0rating, comments0date, comments0comment from heavyBidding.table6 where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "'");
            calculateAFM("ViewUserInfo:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + userId + "'");
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
        String q1 = "select users0nickname from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select * from heavyBidding.table3 where items0id = 'items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
            calculateAFM("BuyNow:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "'");
            calculateAFM("BuyNow:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreBuyNow(int number, BufferedWriter writer) throws Exception {
        String q1 = "select items0quantity, items0nb_of_bids, items0end_date from heavyBidding.table3 where items0id ='items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            int items0id = getRandomNumber(itemsNumber);
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + items0id + "'");
            calculateAFM("StoreBuyNow:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Map<String, String> map = new HashMap<>();
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                map.put("items0id", "items0id" + String.valueOf(items0id));
                long l = updateItem1(map, 1);
                d += l;
                sum += l;
                l = insertBuyNowQuery(String.valueOf(items0id));
                d += l;
                sum += l;
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public long updateItem1(Map<String, String> fields, int index) {
        long times = 0;
        String where = fields.get("items0id");

        // update Table 2
        String q = "select users0id, bids0id from heavyBidding.table2 where items0id='" + where + "';";
        Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q);
        calculateAFM("StoreBuyNow:table2", tuple3._3());
        times += (tuple3._1());
        for (Row row : tuple3._2()) {
            q = "update heavyBidding.table2 set items0end_date='" + fields.get("items0end_date") + "' where items0id='" + where
                    + "' AND bids0id='" + row.getString("bids0id") + "' AND users0id='" + row.getString("users0id") + "';";
            calculateAFM("StoreBuyNow:table2", 1);
            times += (UpdateQuery(q));
        }
        // update: Table 3 
        String items0end_date = fields.get("items0end_date");
        q = "update heavyBidding.table3 set items0quantity='" + fields.get("items0quantity") + "', items0nb_of_bids='" + fields.get("items0nb_of_bids") + "' where items0id='" + where + "' AND items0end_date='" + items0end_date + "';";
        calculateAFM("StoreBuyNow:table3", 1);
        times += (UpdateQuery(q));

        return times;
    }

    public double readPutBid(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select * from heavyBidding.table3 where items0id = 'items0id";
        String q3 = "select bids0id from heavyBidding.table2 where items0id = 'items0id";

        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
            calculateAFM("PutBid:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            tuple3 = execRecord(q2 + String.valueOf(itemId) + "'");
            calculateAFM("PutBid:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + itemId + "'");
            calculateAFM("PutBid:table2", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String q4 = "select bids0qty, bids0date from heavyBidding.mv_table22 where bids0id ='"
                        + row.getString("bids0id") + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q4);
                calculateAFM("PutBid:mv_table22", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreBid(int number, BufferedWriter writer) throws Exception {
        long d = 0;
        double sum = 0;
        Map<String, String> map = new HashMap<>();
        String items0id = String.valueOf(getRandomNumber(itemsNumber));
        map.put("items0id", "items0id" + items0id);
        long l = insertBidsQuery(map);
        d += l;
        sum += l;
        String q1 = "select items0nb_of_bids, items0max_bid, items0end_date from heavyBidding.table3 where items0id ='items0id";
        for (int i = 0; i < number; i++) {
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + items0id + "'");
            calculateAFM("StoreBid:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                    map.put(definition.getName(), row.getString(definition.getName()));
                }
                l = updateItem(map);
                d += l;
                sum += l;

            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readPutComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname, users0password from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select * from heavyBidding.table3 where items0id ='items0id";
        String q3 = "select * from heavyBidding.table5 where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "'");
            calculateAFM("PutComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "'");
            calculateAFM("PutComment:table3", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + userId + "'");
            calculateAFM("PutComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0rating from heavyBidding.table5 where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String user0id = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + user0id + "'");
            calculateAFM("StoreComment:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String users0rating = row.getString("users0rating");
                String q = "update heavyBidding.table5 set users0rating='" + users0rating + "' where users0id='users0id" + user0id + "';";
                long l = UpdateQuery(q);
                calculateAFM("StoreComment:table5", 1);
                d += l;
                sum += l;
                Map<String, String> map = new HashMap<>();
                map.put("users0id", user0id);
                map.put("users0rating", users0rating);
                long k = insertCommentsQuery(map);
                d += k;
                sum += k;
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readAboutMe(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from heavyBidding.table5 where users0id ='users0id";
        String q2 = "select * from heavyBidding.table6 where users0id ='users0id";
        String q3 = "select users0nickname from heavyBidding.table8 where comments0id ='comments0id";

        String q4 = "select * from heavyBidding.table9 where users0id='users0id";
        String q41 = "select * from heavyBidding.table3 where items0id='";
        String q42 = "select * from heavyBidding.table92 where buy_now0id='";

        String q5 = "select items0id from heavyBidding.table10 where users0id ='users0id";
        String q51 = "select * from heavyBidding.table3 where items0id ='";

        String q6 = "select items0id from heavyBidding.table11 where users0id = 'users0id";
        String q61 = "select * from heavyBidding.table3 where items0id = '";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            int userId = getRandomNumber(usersNumber);
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(userId) + "'");
            calculateAFM("AboutMe:table5", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + userId + "'");
            calculateAFM("AboutMe:table6", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + String.valueOf(getRandomNumber(commentsNumber)) + "'");
            calculateAFM("AboutMe:table8", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q4 + userId + "';");
            calculateAFM("AboutMe:table9", tuple3._3());
            sum += d;
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple1 = execRecord(q41 + row.getString("items0id") + "'");
                calculateAFM("AboutMe:table3", tmpTuple1._3());
                d += tmpTuple1._1();
                sum += tmpTuple1._1();
//                for (Row row1 : tmpTuple1._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple2 = execRecord(q42 + row.getString("buy_now0id") + "';"); // AND buy_now0date > '" + row.getString("buy_now0date") + "'
                calculateAFM("AboutMe:table92", tmpTuple2._3());
                d += tmpTuple2._1();
                sum += tmpTuple2._1();
//                }

            }
            tuple3 = execRecord(q5 + userId + "'");
            calculateAFM("AboutMe:table10", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q51 + row.getString("items0id") + "'");
                calculateAFM("AboutMe:table3", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            tuple3 = execRecord(q6 + userId + "'");
            calculateAFM("AboutMe:table11", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q61 + row.getString("items0id") + "'");
                calculateAFM("AboutMe:table3", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readSearchItemsByRegion(int number, BufferedWriter writer) throws Exception {
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String region_cat = entityIds.get("table12").get(getRandomNumber(entityIds.get("table12").size()));
            String regions0id = region_cat.split(":")[0];
            String categories0id = region_cat.split(":")[1];
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord("select * from heavyBidding.table12 where regions0id = '" + regions0id + "' and categories0id = '" + categories0id + "'");
            calculateAFM("SearchItemsByRegion:table12", tuple3._3());
            sum += tuple3._1();
            d += tuple3._1();
            for (Row row : tuple3._2()) {
                String q = "select * from heavyBidding.table3 where items0id='" + row.getString("items0id") + "';"; //+ "' AND items0end_date >='" + row.getString("items0end_date") 
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q);
                calculateAFM("SearchItemsByRegion:table3", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readBrowseRegions(int number, BufferedWriter writer) throws Exception {
        String q1 = "select regions0id, regions0name from heavyBidding.table13 where regions0dummy = '0'";
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
