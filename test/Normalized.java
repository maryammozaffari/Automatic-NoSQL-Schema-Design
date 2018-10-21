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


public class Normalized {

    private Cluster cluster;
    private Session session;
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

    public Normalized() {
        regionAddress = "/home/user/regions.txt";
        categoryAddress = "/home/user/categories.txt";
        usersNumber = 150000;
        itemsNumber = 40000;
        commentsNumber = 110000;
        bidsNumber = 300000;
        buysNumber = 180000;
        categoryNumber = 100;
        regionNumber = 62;
        afm = new HashMap<>();
        dr = new HashMap<>();
        // bidding
        readBrowseCategories = 6617;
        viewBidHistory = 1335;
        viewItem = 12265;
        searchItemsByCategory = 13790;
        viewUserInfo = 2150;
        registerItem = 462;
        registerUser = 929;
        buyNow = 1005;
        storeBuyNow = 956;
        putBid = 4670;
        storeBid = 3237;
        putComment = 402;
        storeComment = 389;
        aboutMe = 1477;
        searchItemsByRegion = 5489;
        browseRegions = 1963;

        // light Bidding
//        readBrowseCategories = 6617;
//        viewBidHistory = 1335;
//        viewItem = 12265;
//        searchItemsByCategory = 13790;
//        viewUserInfo = 2150;
//        registerItem = 5;
//        registerUser = 9;
//        buyNow = 1005;
//        storeBuyNow = 10;
//        putBid = 4670;
//        storeBid = 32;
//        putComment = 402;
//        storeComment = 4;
//        aboutMe = 1477;
//        searchItemsByRegion = 5489;
//        browseRegions = 1963;
        // heavy Bidding
//        readBrowseCategories = 6617;
//        viewBidHistory = 1335;
//        viewItem = 12265;
//        searchItemsByCategory = 13790;
//        viewUserInfo = 2150;
//        registerItem = 46200;
//        registerUser = 92900;
//        buyNow = 1005;
//        storeBuyNow = 95600;
//        putBid = 4670;
//        storeBid = 323700;
//        putComment = 402;
//        storeComment = 38900;
//        aboutMe = 1477;
//        searchItemsByRegion = 5489;
//        browseRegions = 1963;
    }

    public static void main(String[] args) throws Exception {

        String biddingNormalizedbaseAdd = "/home/user/biddingNormalized";
        Normalized mainNormalized = new Normalized();
        mainNormalized.connect("192.168.82.212");
        mainNormalized.getSession();
        mainNormalized.createSchema("normalized");
        mainNormalized.createSchemaAndInsertData();
        mainNormalized.randomRun(biddingNormalizedbaseAdd);
        mainNormalized.writeAfmAndDrToFile(biddingNormalizedbaseAdd);
        mainNormalized.close();

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
                System.out.println("biddingNormalized : " + st + ": " + countMap.get(st));
            }
            if (countMap.get(st) == 0) {
                countMap.remove(st);
                writerMap.get(st)._1().write("======================================\nAvg Time: ");
                double avg = sumMap.get(st) / writerMap.get(st)._2();
                writerMap.get(st)._1().write(String.valueOf(avg));
                writerMap.get(st)._1().close();
                System.out.println("biddingNormalized : " + st + " finished!");
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
        createSchema("normalized");
        loadUserQueryToCassandra(usersNumber, false);
        System.out.println("loadUserQueryToCassandra");
        loadCommentsQueryToCassandra(commentsNumber, usersNumber);
        System.out.println("loadCommentsQueryToCassandra");
        loadItemQueryToCassandra(itemsNumber, false);
        System.out.println("loadItemQueryToCassandra");
        loadBuyNowQueryToCassandra(buysNumber, usersNumber, itemsNumber);
        System.out.println("loadBuyNowQueryToCassandra");
        loadBidsQueryToCassandra(bidsNumber, usersNumber, itemsNumber);
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

        String q1 = "SELECT users0id FROM normalized.users;";
        ResultSet results1 = session.execute(q1);
        currentUsersNumber = -1;
        for (Row row : results1) {
            if (currentUsersNumber < Integer.valueOf(row.getString(0).replaceAll("users0id", ""))) {
                currentUsersNumber = Integer.valueOf(row.getString(0).replaceAll("users0id", ""));
            }
        }
        q1 = "SELECT items0id FROM normalized.items;";
        results1 = session.execute(q1);
        currentItemsNumber = -1;
        for (Row row : results1) {
            if (currentItemsNumber < Integer.valueOf(row.getString(0).replaceAll("items0id", ""))) {
                currentItemsNumber = Integer.valueOf(row.getString(0).replaceAll("items0id", ""));
            }
        }

        q1 = "SELECT bids0id FROM normalized.bids;";
        results1 = session.execute(q1);
        currentBidsNumber = -1;
        for (Row row : results1) {
            if (currentBidsNumber < Integer.valueOf(row.getString(0).replaceAll("bids0id", ""))) {
                currentBidsNumber = Integer.valueOf(row.getString(0).replaceAll("bids0id", ""));
            }
        }
        q1 = "SELECT comments0id FROM normalized.comments;";
        results1 = session.execute(q1);
        currentCommentNumber = -1;
        for (Row row : results1) {
            if (currentCommentNumber < Integer.valueOf(row.getString(0).replaceAll("comments0id", ""))) {
                currentCommentNumber = Integer.valueOf(row.getString(0).replaceAll("comments0id", ""));
            }
        }
        q1 = "SELECT buy_now0id FROM normalized.buy_now;";
        results1 = session.execute(q1);
        currentBuyNumber = -1;
        for (Row row : results1) {
            if (currentBuyNumber < Integer.valueOf(row.getString(0).replaceAll("buy_now0id", ""))) {
                currentBuyNumber = Integer.valueOf(row.getString(0).replaceAll("buy_now0id", ""));
            }
        }
//        System.out.println("JJJ");
    }

    public void createSchema(String ksName) throws Exception {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use " + ksName);
        session.execute("CREATE TABLE IF NOT EXISTS  regions (regions0id text,regions0name text, primary KEY (regions0id)) ;");
        session.execute("CREATE TABLE  IF NOT EXISTS categories (categories0id text,categories0name text, primary KEY (categories0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  all_regions (regions0dummy text, regions0id text,regions0name text, primary KEY (regions0dummy, regions0id)) ;");
        session.execute("CREATE TABLE  IF NOT EXISTS all_categories (categories0dummy text, categories0id text,categories0name text, primary KEY (categories0dummy, categories0id));");
        session.execute("CREATE TABLE IF NOT EXISTS bids_by_item (bids0id text, items0id text, primary KEY (items0id, bids0id));");
        session.execute("CREATE TABLE IF NOT EXISTS items_by_category (categories0id text,items0id text, primary KEY (categories0id, items0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  items_by_region (regions0id text, categories0id text,items0id text, users0id text, primary KEY (regions0id, categories0id, items0id, users0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  comments_recieve_user (users0id text, comments0id text, primary KEY (users0id,comments0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  comments_send_user (users0id text, comments0id text, primary KEY (users0id,comments0id));");
        session.execute("CREATE TABLE IF NOT EXISTS  items (items0id text, items0end_date text, items0name text, items0max_bid text, items0quantity text,  items0reserve_price text, items0nb_of_bids text, items0start_date text, items0description text, items0initial_price text,items0buy_now text, primary KEY (items0id, items0end_date)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  comments (to_users0id text, from_users0id text, comments0id text, comments0comment text, comments0date text, comments0rating text, primary KEY (comments0id,to_users0id,from_users0id)) ;");
        session.execute("CREATE TABLE IF NOT EXISTS  users (regions0id text, users0id text,users0email text, users0nickname text, users0lastname text, users0rating text, users0creation_date text, users0firstname text, users0password text, users0balance text, primary KEY (users0id,regions0id)) ;");
        session.execute("CREATE TABLE  IF NOT EXISTS bids (bids0id text,users0id text, items0id text, bids0date text, bids0bid text, bids0qty text, primary KEY (bids0id, users0id, items0id, bids0date));");
        session.execute("CREATE TABLE IF NOT EXISTS  buy_now (buy_now0id text,items0id text, buy_now0date text, buy_now0qty text, primary KEY (buy_now0id, items0id, buy_now0date));");
        session.execute("CREATE TABLE IF NOT EXISTS  items_sell_users (users0id text, items0id text, primary KEY (users0id, items0id));");
        session.execute("CREATE TABLE IF NOT EXISTS buynow_by_user (users0id text,buy_now0id text, primary KEY (users0id, buy_now0id)) ;");
        session.execute("CREATE TABLE  IF NOT EXISTS bids_by_user (users0id text, bids0id text, primary KEY (users0id, bids0id));");

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
                session.execute("INSERT INTO all_regions (regions0dummy ,regions0id ,regions0name) VALUES ('0','" + i + "','" + line + "');");
                session.execute("INSERT INTO regions (regions0id ,regions0name) VALUES ('" + i + "','" + line + "');");
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
                session.execute("INSERT INTO all_categories (categories0dummy,categories0id ,categories0name) VALUES ('0','" + i + "','" + line + "');");
                session.execute("INSERT INTO categories (categories0id ,categories0name) VALUES ('" + i + "','" + line + "');");
            }
        }
        reader.close();
        return i;
    }

    public Long insertRecord(String tablename, String feilds, String data) throws Exception {
        long s = new Date().getTime();
//        String q = "INSERT INTO normalized." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");";
//        System.out.println("INSERT INTO normalized." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        session.execute("INSERT INTO normalized." + tablename + " (" + feilds + ") " + "VALUES (" + data + ");");
        long e = new Date().getTime();
        return e - s;
    }

    public List<Long> loadItemQueryToCassandra(int itemCount, boolean flag) throws Exception {
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            long d = 0;
            int userRand = getRandomNumber(usersNumber);
            int catRand = getRandomNumber(categoryNumber);
            String feilds = "items0id, items0end_date , items0name , items0max_bid , items0quantity ,  items0reserve_price, items0nb_of_bids, items0start_date, items0description, items0initial_price,items0buy_now";
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
            d += (insertRecord("items", feilds, builder.toString()));
            if (flag) {
                calculateDr(feilds);
                calculateAFM("RegisterItem:items", 1);
            }

// insert into table items_by_category
            feilds = "categories0id ,items0id";
            StringBuilder builderTbl4 = new StringBuilder();
            builderTbl4.append("'categories0id").append(catRand).append("',");
            builderTbl4.append("'items0id").append(i).append("'");
            d += (insertRecord("items_by_category", feilds, builderTbl4.toString()));
//            entityIds.get("items_by_category:categories").add(catRand + ":" + i);
            if (flag) {
                calculateDr(feilds);
                calculateAFM("RegisterItem:items_by_category", 1);
            }

            // insert into table items_sell_users
            feilds = "users0id, items0id";
//            entityIds.get("items_sell_users:users").add(String.valueOf(userRand));
            StringBuilder builderTbl10 = new StringBuilder();
            builderTbl10.append("'users0id").append(userRand).append("',");
            builderTbl10.append("'items0id").append(i).append("'");
            d += (insertRecord("items_sell_users", feilds, builderTbl10.toString()));
            if (flag) {
                calculateDr(feilds);
                calculateAFM("RegisterItem:items_sell_users", 1);
            }

            // insert into table items_by_region
            feilds = "regions0id, categories0id ,items0id, users0id";
            StringBuilder builderTbl12 = new StringBuilder();
//            entityIds.get("items_by_region:users").add(String.valueOf(userRand));

            String regions0id = getRecord("users", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");

            builderTbl12.append("'").append(regions0id).append("',");
            builderTbl12.append("'categories0id").append(catRand).append("',");
            builderTbl12.append("'items0id").append(i).append("',");
            builderTbl12.append("'users0id").append(userRand).append("'");
            d += (insertRecord("items_by_region", feilds, builderTbl12.toString()));
//            entityIds.get("items_by_region:regions").add(String.valueOf(regions0id + ":" + catRand));
            if (flag) {
                calculateDr(feilds);
                calculateAFM("RegisterItem:items_by_region", 1);
            }
            times.add(d);
        }
        return times;
    }

    public Long insertItemQuery() throws Exception {
        long d = 0;
        currentItemsNumber++;
        int i = currentItemsNumber;
        int userRand = getRandomNumber(usersNumber);
        int catRand = getRandomNumber(categoryNumber);
        String feilds = "items0id, items0end_date , items0name , items0max_bid , items0quantity ,  items0reserve_price, items0nb_of_bids, items0start_date, items0description, items0initial_price,items0buy_now";
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
        d += (insertRecord("items", feilds, builder.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:items", 1);

// insert into table items_by_category
        feilds = "categories0id ,items0id";
        StringBuilder builderTbl4 = new StringBuilder();
        builderTbl4.append("'categories0id").append(catRand).append("',");
        builderTbl4.append("'items0id").append(i).append("'");
        d += (insertRecord("items_by_category", feilds, builderTbl4.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:items_by_category", 1);

        // insert into table items_sell_users
        feilds = "users0id, items0id";
        StringBuilder builderTbl10 = new StringBuilder();
        builderTbl10.append("'users0id").append(userRand).append("',");
        builderTbl10.append("'items0id").append(i).append("'");
        d += (insertRecord("items_sell_users", feilds, builderTbl10.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:items_sell_users", 1);

        // insert into table items_by_region
        feilds = "regions0id, categories0id ,items0id, users0id";
        StringBuilder builderTbl12 = new StringBuilder();

        String regions0id = getRecord("users", "users0id", "users0id" + String.valueOf(userRand)).get("regions0id");

        builderTbl12.append("'").append(regions0id).append("',");
        builderTbl12.append("'categories0id").append(catRand).append("',");
        builderTbl12.append("'items0id").append(i).append("',");
        builderTbl12.append("'users0id").append(userRand).append("'");
        d += (insertRecord("items_by_region", feilds, builderTbl12.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterItem:items_by_region", 1);
        return d;
    }

    public List<Long> loadBuyNowQueryToCassandra(int buyNumber, int userNumber, int itemNumber) throws Exception {
        List<Long> times = new ArrayList<>();
        // insert into table buy_now
        for (int i = 0; i < buyNumber; i++) {
            int userRand = getRandomNumber(userNumber);
            int itemRand = getRandomNumber(itemNumber);
//            entityIds.get("buy_now:users").add(String.valueOf(userRand));
//            entityIds.get("buy_now:items").add(String.valueOf(itemRand));
            String feilds = "buy_now0date, items0id, buy_now0id, buy_now0qty";
            StringBuilder builderTbl9 = new StringBuilder();
            builderTbl9.append("'buy_now0date").append(i).append("',");
            builderTbl9.append("'items0id").append(itemRand).append("',");
            builderTbl9.append("'buy_now0id").append(i).append("',");
            builderTbl9.append("'buy_now0qty").append(i).append("'");
            times.add(insertRecord("buy_now", feilds, builderTbl9.toString()));

//            // insert into stable buynow_by_user
            feilds = "buy_now0id, users0id";
//            entityIds.get("buynow_by_user:users").add(String.valueOf(itemRand));
            StringBuilder builderTbl16 = new StringBuilder();
            builderTbl16.append("'buy_now0id").append(i).append("',");
            builderTbl16.append("'users0id").append(userRand).append("'");
            times.add(insertRecord("buynow_by_user", feilds, builderTbl16.toString()));
        }
        return times;
    }

    public long insertBuyNowQuery(String itemId) throws Exception {
        long times = 0;
        // insert into table buy_now
        currentBuyNumber++;
        int buyRand = currentBuyNumber;
        int userRand = getRandomNumber(usersNumber);
        String feilds = "buy_now0date, items0id, buy_now0id, buy_now0qty";
        StringBuilder builderTbl9 = new StringBuilder();
        builderTbl9.append("'buy_now0date").append(buyRand).append("',");
        builderTbl9.append("'items0id").append(itemId).append("',");
        builderTbl9.append("'buy_now0id").append(buyRand).append("',");
        builderTbl9.append("'buy_now0qty").append(buyRand).append("'");
        times += (insertRecord("buy_now", feilds, builderTbl9.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBuyNow:buy_now", 1);

        // insert into stable buynow_by_user
        feilds = "buy_now0id, users0id";
//        entityIds.get("buynow_by_user:users").add(String.valueOf(itemId));
        StringBuilder builderTbl16 = new StringBuilder();
        builderTbl16.append("'buy_now0id").append(buyRand).append("',");
        builderTbl16.append("'users0id").append(userRand).append("'");
        calculateDr(feilds);
        times += (insertRecord("buynow_by_user", feilds, builderTbl16.toString()));
        calculateAFM("StoreBuyNow:buynow_by_user", 1);
        return times;
    }

    public List<Long> loadBidsQueryToCassandra(int bidNumber, int userNumber, int itemNumber) throws Exception {
        List<Long> times = new ArrayList<>();
        // insert into table bids
        for (int i = 0; i < bidNumber; i++) {
            int itemRand = getRandomNumber(itemNumber);
            int userRand = getRandomNumber(userNumber);
//            entityIds.get("bids:items").add(String.valueOf(itemRand));
//            entityIds.get("bids:users").add(String.valueOf(userRand));

            String feilds = "items0id, bids0id, users0id, bids0date,bids0bid,bids0qty";
            StringBuilder builderTbl2 = new StringBuilder();
            builderTbl2.append("'items0id").append(itemRand).append("',");
            builderTbl2.append("'bids0id").append(i).append("',");
            builderTbl2.append("'users0id").append(userRand).append("',");
            builderTbl2.append("'bids0date").append(i).append("',");
            builderTbl2.append("'bids0bid").append(i).append("',");
            builderTbl2.append("'bids0qty").append(i).append("'");
            times.add(insertRecord("bids", feilds, builderTbl2.toString()));

            // insert into table bids_by_item
            feilds = "bids0id, items0id";
            StringBuilder builderTbl22 = new StringBuilder();
            builderTbl22.append("'bids0id").append(i).append("',");
            builderTbl22.append("'items0id").append(itemRand).append("'");
            times.add(insertRecord("bids_by_item", feilds, builderTbl22.toString()));
//            entityIds.get("bids_by_item:items").add(String.valueOf(itemRand));

            // insert into table bids_by_user
            feilds = "users0id, bids0id";
            StringBuilder builderTbl11 = new StringBuilder();
            builderTbl11.append("'users0id").append(userRand).append("',");
            builderTbl11.append("'bids0id").append(i).append("'");
            times.add(insertRecord("bids_by_user", feilds, builderTbl11.toString()));
        }
        return times;
    }

    public Long insertBidsQuery(Map<String, String> map) throws Exception {
        long times = 0;

        currentBidsNumber++;
        int bidRand = currentBidsNumber;
        int userRand = getRandomNumber(usersNumber);
        // insert into table bids
        String feilds = "items0id, bids0id, users0id, bids0date, bids0bid, bids0qty";
        calculateDr(feilds);
        StringBuilder builderTbl2 = new StringBuilder();
        builderTbl2.append("'").append(map.get("items0id")).append("',");
        builderTbl2.append("'bids0id").append(bidRand).append("',");
        builderTbl2.append("'users0id").append(userRand).append("',");
        builderTbl2.append("'bids0date").append(bidRand).append("',");
        builderTbl2.append("'bids0bid").append(bidRand).append("',");
        builderTbl2.append("'bids0qty").append(bidRand).append("'");
        times += (insertRecord("bids", feilds, builderTbl2.toString()));
        calculateAFM("StoreBid:bids", 1);

        // insert into table bids_by_item
        feilds = "bids0id, items0id";
        StringBuilder builderTbl22 = new StringBuilder();
        builderTbl22.append("'bids0id").append(bidRand).append("',");
        builderTbl22.append("'").append(map.get("items0id")).append("'");
        times += (insertRecord("bids_by_item", feilds, builderTbl22.toString()));
//        entityIds.get("bids_by_item:items").add(String.valueOf(map.get("items0id")));
        calculateDr(feilds);
        calculateAFM("StoreBid:bids_by_item", 1);

        // insert into table bids_by_user
        feilds = "users0id, bids0id";
        StringBuilder builderTbl11 = new StringBuilder();
        builderTbl11.append("'users0id").append(userRand).append("',");
        builderTbl11.append("'bids0id").append(bidRand).append("'");
        times += (insertRecord("bids_by_user", feilds, builderTbl11.toString()));
        calculateDr(feilds);
        calculateAFM("StoreBid:bids_by_user", 1);

        return times;
    }

    public List<Long> loadCommentsQueryToCassandra(int commentCount, int userNmuber) throws Exception {
        List<Long> times = new ArrayList<>();
        // insert into table comments
        for (int i = 0; i < commentCount; i++) {
            int toUserRand = getRandomNumber(userNmuber);
            int fromUserRand = getRandomNumber(userNmuber);
//            entityIds.get("comments:to_users").add(String.valueOf(toUserRand));
//            entityIds.get("comments:from_users").add(String.valueOf(fromUserRand));
//            entityIds.get("comments_recieve_user:users").add(String.valueOf(fromUserRand));
//            entityIds.get("comments_send_user:users").add(String.valueOf(toUserRand));

            String feilds = "comments0id, to_users0id, from_users0id, comments0comment, comments0date, comments0rating";
            StringBuilder builderTbl6 = new StringBuilder();
            builderTbl6.append("'comments0id").append(i).append("',");
            builderTbl6.append("'to_users0id").append(toUserRand).append("',");
            builderTbl6.append("'from_users0id").append(fromUserRand).append("',");
            builderTbl6.append("'comments0comment").append(i).append("',");
            builderTbl6.append("'comments0date").append(i).append("',");
            builderTbl6.append("'comments0rating").append(i).append("'");
            times.add(insertRecord("comments", feilds, builderTbl6.toString()));

            // insert into table comments_recieve_user
            feilds = "users0id, comments0id";
            StringBuilder builderTbl7 = new StringBuilder();
            builderTbl7.append("'users0id").append(fromUserRand).append("',");
            builderTbl7.append("'comments0id").append(i).append("'");
            times.add(insertRecord("comments_recieve_user", feilds, builderTbl7.toString()));

            // insert into table comments_send_user
            feilds = " comments0id, users0id";
            StringBuilder builderTbl8 = new StringBuilder();
            builderTbl8.append("'comments0id").append(i).append("',");
            builderTbl8.append("'users0id").append(toUserRand).append("'");
            times.add(insertRecord("comments_send_user", feilds, builderTbl8.toString()));
        }
        return times;
    }

    public Long insertCommentsQuery(Map<String, String> map) throws Exception {
        long times = 0;
        // insert into table comment
        currentCommentNumber++;
        int commentRand = currentCommentNumber;
        int toUserRand = getRandomNumber(usersNumber);
        int fromUserRand = getRandomNumber(usersNumber);
        String feilds = "comments0id, to_users0id, from_users0id, comments0comment, comments0date, comments0rating";
        calculateDr(feilds);
        StringBuilder builderTbl6 = new StringBuilder();
        builderTbl6.append("'comments0id").append(commentRand).append("',");
        builderTbl6.append("'to_users0id").append(toUserRand).append("',");
        builderTbl6.append("'from_users0id").append(fromUserRand).append("',");
        builderTbl6.append("'comments0comment").append(commentRand).append("',");
        builderTbl6.append("'comments0date").append(commentRand).append("',");
        builderTbl6.append("'comments0rating").append(commentRand).append("'");
        times += (insertRecord("comments", feilds, builderTbl6.toString()));
        calculateAFM("StoreComment:comments", 1);

        // insert into table comments_recieve_user
        feilds = "users0id, comments0id";
        StringBuilder builderTbl7 = new StringBuilder();
        builderTbl7.append("'users0id").append(fromUserRand).append("',");
        builderTbl7.append("'comments0id").append(commentRand).append("'");
        times += (insertRecord("comments_recieve_user", feilds, builderTbl7.toString()));
        calculateAFM("StoreComment:comments_recieve_user", 1);
        calculateDr(feilds);

        // insert into table comments_send_user
        feilds = " comments0id, users0id";
        StringBuilder builderTbl8 = new StringBuilder();
        builderTbl8.append("'comments0id").append(commentRand).append("',");
        builderTbl8.append("'users0id").append(toUserRand).append("'");
        times += (insertRecord("comments_send_user", feilds, builderTbl8.toString()));
        calculateAFM("StoreComment:comments_send_user", 1);
        calculateDr(feilds);
        return times;
    }

    public List<Long> loadUserQueryToCassandra(int userCount, boolean flag) throws Exception {
        List<Long> times = new ArrayList<>();
        //   Insert to Table5
        for (int i = 0; i < userCount; i++) {
            long d = 0;
            int regions0idRand = getRandomNumber(regionNumber);
//            entityIds.get("users:regions").add(String.valueOf(regions0idRand));

            String feilds = "users0id,regions0id,users0email, users0nickname, users0lastname, users0rating, users0creation_date, users0firstname, users0password,users0balance";
            StringBuilder builder5 = new StringBuilder();
            builder5.append("'users0id").append(i).append("',");
            builder5.append("'regions0id").append(regions0idRand).append("',");
            builder5.append("'users0email").append(i).append("',");
            builder5.append("'users0nickname").append(i).append("',");
            builder5.append("'users0lastname").append(i).append("',");
            builder5.append("'users0rating").append(i).append("',");
            builder5.append("'users0creation_date").append(i).append("',");
            builder5.append("'users0firstname").append(i).append("',");
            builder5.append("'users0password").append(i).append("',");
            builder5.append("'users0balance").append(i).append("'");
            d += (insertRecord("users", feilds, builder5.toString()));
            if (flag) {
                calculateDr(feilds);
                calculateAFM("RegisterUser:users", 1);
            }
            times.add(d);
        }
        return times;
    }

    public Long insertUserQuery() throws Exception {
        long times = 0;
        //   Insert to Table5
        long d = 0;
        currentUsersNumber++;
        int i = currentUsersNumber;
        int regions0idRand = getRandomNumber(regionNumber);
        String feilds = "users0id,regions0id,users0email, users0nickname, users0lastname, users0rating, users0creation_date, users0firstname, users0password,users0balance";
        StringBuilder builder5 = new StringBuilder();
        builder5.append("'users0id").append(i).append("',");
        builder5.append("'regions0id").append(regions0idRand).append("',");
        builder5.append("'users0email").append(i).append("',");
        builder5.append("'users0nickname").append(i).append("',");
        builder5.append("'users0lastname").append(i).append("',");
        builder5.append("'users0rating").append(i).append("',");
        builder5.append("'users0creation_date").append(i).append("',");
        builder5.append("'users0firstname").append(i).append("',");
        builder5.append("'users0password").append(i).append("',");
        builder5.append("'users0balance").append(i).append("'");
        d += (insertRecord("users", feilds, builder5.toString()));
        calculateDr(feilds);
        calculateAFM("RegisterUser:users", 1);
        times += d;
        return times;
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

    public Long updateItem(Map<String, String> fields, int index) {
        long times = 0;
        String itemId = fields.get("items0id");
        // update: Table items
        String tmp = "items0nb_of_bids='" + fields.get("items0nb_of_bids") + "',items0max_bid='" + fields.get("items0max_bid") + "'";
        String q = "update normalized.items set " + tmp + " where items0id='" + itemId
                + "' AND items0end_date='" + fields.get("items0end_date") + "';";
        calculateAFM("StoreBid:items", 1);
        times = UpdateQuery(q);
        return times;
    }

    public Map<String, String> getRecord(String tblName, String field, String value) {
        Map<String, String> map = new HashMap<>();
        String q = "SELECT * FROM normalized." + tblName + " WHERE  " + field + " = '" + value + "';";
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
        String q1 = "select users0nickname, users0password from normalized.users where users0id ='users0id";
        String q2 = "select categories0id from normalized.all_categories where categories0dummy = '0'";
        String q3 = "select categories0name from normalized.categories where categories0id='";
        double sum = 0;

        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
            calculateAFM("BrowseCategories:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2);
            calculateAFM("BrowseCategories:all_categories", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3 + row.getString(0) + "';");
                calculateAFM("BrowseCategories:categories", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewBidHistory(int number, BufferedWriter writer) throws Exception {
        String q1 = "select items0name from normalized.items where items0id='items0id";
        String q2 = "select bids0id from normalized.bids_by_item where items0id= 'items0id";
        String q3 = "select bids0qty, bids0date,bids0bid, users0id from normalized.bids where bids0id='";
        String q4 = "select users0id, users0nickname from normalized.users where users0id='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "'");
            calculateAFM("ViewBidHistory:items", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "'");
            calculateAFM("ViewBidHistory:bids_by_item", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple1 = execRecord(q3 + row.getString(0) + "'");
                calculateAFM("ViewBidHistory:bids", tmpTuple1._3());
                d += tmpTuple1._1();
                sum += tmpTuple1._1();
                for (Row row1 : tmpTuple1._2()) {
                    Tuple3<Long, List<Row>, Integer> tmpTuple2 = execRecord(q4 + row1.getString("users0id") + "'");
                    calculateAFM("ViewBidHistory:users", tmpTuple2._3());
                    d += tmpTuple2._1();
                    sum += tmpTuple2._1();
                }
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }

        return sum;
    }

    public double readViewItem(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from normalized.items where items0id = 'items0id";
        String q2 = "select bids0id from normalized.bids_by_item where items0id ='items0id";
        String q3 = "select bids0qty, bids0bid, bids0date from normalized.bids where bids0id= ";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "'");
            calculateAFM("ViewItem:items", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + itemId + "'");
            calculateAFM("ViewItem:bids_by_item", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row1 : tuple3._2()) {
                String str = q3 + "'" + row1.getString(0) + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple2 = execRecord(str);
                calculateAFM("ViewItem:bids", tmpTuple2._3());
                d += tmpTuple2._1();
                sum += tmpTuple2._1();
            }

            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readSearchItemsByCategory(int number, BufferedWriter writer) throws Exception {
        String q1;// = "select * from normalized.table4 where categories0id ='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String category = String.valueOf(getRandomNumber(categoryNumber));
            q1 = "select items0id from normalized.items_by_category where categories0id ='categories0id" + category + "';";
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1);
            calculateAFM("SearchItemsByCategory:items_by_category", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String q3 = "select * from normalized.items where items0id='" + row.getString(0)
                        + "' AND items0end_date>='items0end_date" + row.getString(0).replaceAll("items0id", "") + "' ;";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q3);
                calculateAFM("SearchItemsByCategory:items", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readViewUserInfo(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from normalized.users where users0id ='users0id";
        String q11 = "select comments0id from normalized.comments_recieve_user where users0id ='users0id";
        String q2 = "select comments0id, comments0rating, comments0date, comments0comment from normalized.comments where comments0id ='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String user0id = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + user0id + "'");
            calculateAFM("ViewUserInfo:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q11 + user0id + "'");
            calculateAFM("ViewUserInfo:comments_recieve_user", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q2 + row.getString(0) + "'");
                calculateAFM("ViewUserInfo:comments", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
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
        String q1 = "select users0nickname from normalized.users where users0id ='users0id";
        String q2 = "select * from normalized.items where items0id = 'items0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
            calculateAFM("BuyNow:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "'");
            calculateAFM("BuyNow:items", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreBuyNow(int number, BufferedWriter writer) throws Exception {
        String q1 = "select items0quantity, items0nb_of_bids, items0end_date from normalized.items where items0id ='items0id";
        double sum = 0;
        long d = 0;
        int items0id = getRandomNumber(itemsNumber);
        Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + items0id + "'");
        calculateAFM("StoreBuyNow:items", tuple3._3());
        d += tuple3._1();
        sum += tuple3._1();
        for (Row row : tuple3._2()) {
            Map<String, String> map = new HashMap<>();
            for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
                map.put("items0id", String.valueOf(items0id));
                map.put(definition.getName(), row.getString(definition.getName()));
            }
            long l = updateItem1(map, 1);
            d += l;
            sum += l;
            l = insertBuyNowQuery(String.valueOf(items0id));
            d += l;
            sum += l;
        }
        writer.write(String.valueOf(d));
        writer.write("\n");
        return sum;
    }

    public long updateItem1(Map<String, String> fields, int index) {
        long times = 0;
        // update: Table items 
        String itemId = fields.get("items0id");
        String items0end_date = fields.get("items0end_date");
        String q = "update normalized.items set items0quantity='" + fields.get("items0quantity") + "', items0nb_of_bids='" + fields.get("items0nb_of_bids") + "' where items0id='items0id" + itemId + "' AND items0end_date='" + items0end_date + "';";
        calculateAFM("StoreBuyNow:items", 1);
        times += (UpdateQuery(q));
        return times;
    }

    public double readPutBid(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname from normalized.users where users0id ='users0id";
        String q2 = "select * from normalized.items where items0id = 'items0id";
        String q31 = "select bids0id from normalized.bids_by_item where items0id = 'items0id";
        String q32 = "select * from normalized.bids where bids0id = '";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + String.valueOf(getRandomNumber(usersNumber)) + "'");
            calculateAFM("PutBid:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            String itemId = String.valueOf(getRandomNumber(itemsNumber));
            tuple3 = execRecord(q2 + String.valueOf(itemId) + "'");
            calculateAFM("PutBid:items", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q31 + itemId + "'");
            calculateAFM("PutBid:bids_by_item", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q32 + row.getString(0) + "'");
                calculateAFM("PutBid:bids", tmpTuple._3());
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
        String itemId = String.valueOf(getRandomNumber(itemsNumber));
        Map<String, String> map = new HashMap<>();
        map.put("items0id", "items0id" + itemId);
        long l = insertBidsQuery(map);
        d += l;
        sum += l;
        String q1 = "select items0nb_of_bids, items0max_bid,items0end_date from normalized.items where items0id ='items0id";
        Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + itemId + "'");
        calculateAFM("StoreBid:items", tuple3._3());
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
        return sum;
    }

    public double readPutComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select users0nickname, users0password from normalized.users where users0id ='users0id";
        String q2 = "select * from normalized.items where items0id ='items0id";
        String q3 = "select * from normalized.users where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "'");
            calculateAFM("PutComment:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q2 + String.valueOf(getRandomNumber(itemsNumber)) + "'");
            calculateAFM("PutComment:items", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q3 + userId + "'");
            calculateAFM("PutComment:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readStoreComment(int number, BufferedWriter writer) throws Exception {
        String q1 = "select * from normalized.users where users0id ='users0id";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String user0id = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + user0id + "'");
            calculateAFM("StoreComment:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String users0rating = "users0rating" + getRandomNumber(usersNumber);
                String q = "update normalized.users set users0rating='" + users0rating
                        + "' where users0id='users0id" + user0id + "' AND regions0id='"
                        + row.getString("regions0id") + "';";
                long l = UpdateQuery(q);
                calculateAFM("StoreComment:users", 1);
                d += l;
                sum += l;
                Map<String, String> map = new HashMap<>();
                map.put("users0id", user0id);
                map.put("users0rating", users0rating);
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
        String q1 = "select * from normalized.users where users0id ='users0id";
        String q21 = "select comments0id from normalized.comments_recieve_user where users0id ='users0id";
        String q22 = "select * from normalized.comments where comments0id='";

        String q3 = "select from_users0id from normalized.comments where comments0id ='comments0id";
        String q31 = "select users0nickname from normalized.users where users0id ='";

        String q4 = "select buy_now0id from normalized.buynow_by_user where users0id='users0id";
        String q41 = "select * from normalized.buy_now where buy_now0id='";
        String q42 = "select * from normalized.items where items0id='";

        String q5 = "select items0id from normalized.items_sell_users where users0id='users0id";
        String q51 = "select * from normalized.items where items0id='";

        String q6 = "select bids0id from normalized.bids_by_user where users0id = 'users0id";
        String q61 = "select items0id from normalized.bids where bids0id='";
        String q62 = "select * from normalized.items where items0id='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            String userId = String.valueOf(getRandomNumber(usersNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1 + userId + "'");
            calculateAFM("AboutMe:users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            tuple3 = execRecord(q21 + userId + "'");
            calculateAFM("AboutMe:comments_recieve_user", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q22 + row.getString(0) + "'");
                calculateAFM("AboutMe:comments", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            tuple3 = execRecord(q3 + String.valueOf(getRandomNumber(commentsNumber)) + "'");
            calculateAFM("AboutMe:comments", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String str = q31 + row.getString(0).replaceAll("from_", "") + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(str);
                calculateAFM("AboutMe:users", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            tuple3 = execRecord(q4 + userId + "'");
            calculateAFM("AboutMe:buynow_by_user", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple1 = execRecord(q41 + row.getString(0) + "';");
                calculateAFM("AboutMe:buy_now", tmpTuple1._3());
                d += tmpTuple1._1();
                sum += tmpTuple1._1();
                for (Row row1 : tmpTuple1._2()) {
                    Tuple3<Long, List<Row>, Integer> tmpTuple2 = execRecord(q42 + row1.getString(1) + "'");
                    calculateAFM("AboutMe:items", tmpTuple2._3());
                    d += tmpTuple2._1();
                    sum += tmpTuple2._1();
                }
            }
            tuple3 = execRecord(q5 + userId + "'");
            calculateAFM("AboutMe:items_sell_users", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String str = q51 + row.getString(0)
                        + "' AND items0end_date >= 'items0end_date" + row.getString(0).replaceAll("items0id", "") + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(str);
                calculateAFM("AboutMe:items", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            tuple3 = execRecord(q6 + userId + "'");
            calculateAFM("AboutMe:bids_by_user", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple1 = execRecord(q61 + row.getString(0) + "';");
                calculateAFM("AboutMe:bids", tmpTuple1._3());
                d += tmpTuple1._1();
                sum += tmpTuple1._1();
                for (Row row1 : tmpTuple1._2()) {
                    Tuple3<Long, List<Row>, Integer> tmpTuple2 = execRecord(q62 + row1.getString(0)
                            + "' AND items0end_date > 'items0end_date" + row.getString(0).replaceAll("items0id", "") + " ';");
                    calculateAFM("AboutMe:items", tmpTuple2._3());
                    d += tmpTuple2._1();
                    sum += tmpTuple2._1();
                }
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
            String regions0id = String.valueOf(getRandomNumber(regionNumber));
            String categories0id = String.valueOf(getRandomNumber(categoryNumber));
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord("select items0id from normalized.items_by_region where regions0id = 'regions0id" + regions0id + "' and categories0id = 'categories0id" + categories0id + "'");
            calculateAFM("SearchItemsByRegion:items_by_region", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                String q2 = "select * from normalized.items where items0id='" + row.getString(0) + "' AND items0end_date >= '" + row.getString(0).replaceAll("items0id", "") + "';";
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q2);
                calculateAFM("SearchItemsByRegion:items", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
            writer.write(String.valueOf(d));
            writer.write("\n");
        }
        return sum;
    }

    public double readBrowseRegions(int number, BufferedWriter writer) throws Exception {
        String q1 = "select regions0id from normalized.all_regions where regions0dummy = '0'";
        String q2 = " select regions0name from normalized.regions where regions0id='";
        double sum = 0;
        for (int i = 0; i < number; i++) {
            long d = 0;
            Tuple3<Long, List<Row>, Integer> tuple3 = execRecord(q1);
            calculateAFM("BrowseRegions:all_regions", tuple3._3());
            d += tuple3._1();
            sum += tuple3._1();
            for (Row row : tuple3._2()) {
                Tuple3<Long, List<Row>, Integer> tmpTuple = execRecord(q2 + row.getString(0) + "';");
                calculateAFM("BrowseRegions:regions", tmpTuple._3());
                d += tmpTuple._1();
                sum += tmpTuple._1();
            }
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
