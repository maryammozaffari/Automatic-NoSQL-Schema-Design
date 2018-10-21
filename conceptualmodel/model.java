package model;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author maryam
 */
public class Models {

    public String categories;
    public String regions;
    public String users;
    public String items;
    public String bids;
    public String comments;
    public String buynow;
    public Set<String> entites;
    public Map<String, String> relations;

    public Models() {

        categories = "id name dummy";
        regions = "id name dummy";
        users = "id firstname lastname nickname password email rating balance creation_date";
        items = "id name description initial_price quantity reserve_price buy_now nb_of_bids max_bid start_date end_date";
        bids = "id qty bid date";
        comments = "id rating date comment";
        buynow = "id qty date";

        entites = new HashSet<>();
        entites.add("categories");
        entites.add("regions");
        entites.add("users");
        entites.add("items");
        entites.add("bids");
        entites.add("comments");
        entites.add("buy_now");

        relations = new HashMap<>();
        relations.put("users=regions", "region");
        relations.put("items=users", "sell");
        relations.put("items=categories", "category");
        relations.put("bids=users", "bidding");
        relations.put("bids=items", "bidden");
        relations.put("comments=users", "send_from_user");
        relations.put("comments=users", "recive_to_user");
        relations.put("comments=items", "comment_on");
        relations.put("buy_now=users", "buy");
        relations.put("buy_now=items", "bought");

    }

    public String getCategories() {
        return categories;
    }

    public void setCategories(String categories) {
        this.categories = categories;
    }

    public String getRegions() {
        return regions;
    }

    public void setRegions(String regions) {
        this.regions = regions;
    }

    public String getUsers() {
        return users;
    }

    public void setUsers(String users) {
        this.users = users;
    }

    public String getItems() {
        return items;
    }

    public void setItems(String items) {
        this.items = items;
    }

    public String getBids() {
        return bids;
    }

    public void setBids(String bids) {
        this.bids = bids;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getBuynow() {
        return buynow;
    }

    public void setBuynow(String buynow) {
        this.buynow = buynow;
    }

    public Set<String> getEntites() {
        return entites;
    }

    public Map<String, String> getRelations() {
        return relations;
    }

}
