
import model.Query;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import model.Models;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author vahid
 */
public class Utils {

    

    public static Set<String> getEntitiesPrimaryKeys(Query query) {
        Set<String> entities_primaryKeys = new HashSet<>();
        for (String str : query.getEntities()) {
            entities_primaryKeys.add(str + "id");
        }
        return entities_primaryKeys;
    }

    public static Set<String> getUnion(Set<String> partition_key, Set<String> clustering_key) {
        Set<String> set = new HashSet<>(partition_key);
        set.addAll(clustering_key);
        return set;
    }
    
     public static String getAllEntityFields(Models models, String str) {
        String entity = str.substring(0, str.length() - 2);
        String result = null;
        switch (entity) {
            case "items":
                result = "items." + models.getItems().replaceAll(" ", " ,items.") + ",";
                break;
            case "bids":
                result = "bids." + models.getBids().replaceAll(" ", " ,bids.") + ",";
                break;
            case "users":
                result = "users." + models.getUsers().replaceAll(" ", " ,users.") + ",";
                break;
            case "buy_now":
                result = "buy_now." + models.getBuynow().replaceAll(" ", " ,buy_now.") + ",";
                break;
            case "categories":
                result = "categories." + models.getCategories().replaceAll(" ", " ,categories.") + ",";
                break;
            case "regions":
                result = "regions." + models.getRegions().replaceAll(" ", " ,regions.") + ",";
                break;
            case "comments":
                result = "comments." + models.getComments().replaceAll(" ", " ,comments.") + ",";
                break;
        }

        return result;
    }
     public static void writeSupportTablesToFile(List<Query> quereis, String path) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            for (Query query : quereis) {

                if (query.getType().equals("UpdateSupportColumnFamily")) {
                    writer.write("\n\n*************************Update-Support Column Family Query*************************************");
                    writer.write("\nBase Query : " + query.getQuery());
                    writer.write("\nType : " + query.getType());

                    writer.write("\nPartition_keys : " + String.join(", ", query.getPartition_keys()));
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nClustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nClustering_keys : Null ");
                    }
                    writer.write("\nColumn_values : " + String.join(", ", query.getColumn_values()));
                    if (!query.getRelations().isEmpty()) {
                        writer.write("\nRelations : " + String.join(", ", query.getRelations()));
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                    writer.write("\nMSG : " + query.getMsg());
                } else if (query.getType().equals("InsertSupportColumnFamily")) {
                    writer.write("\n\n*************************Insert Support-ColumnFamily Query*************************************");
                    writer.write("\nBase Query : " + query.getQuery());
                    writer.write("\nType : " + query.getType());
                    if (!query.getPartition_keys().isEmpty()) {
                        writer.write("\nPartition_keys : " + String.join(", ", query.getPartition_keys()));
                    } else {
                        writer.write("\nPartition_keys : Null ");
                    }
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nClustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nClustering_keys : Null ");
                    }
                    if (!query.getColumn_values().isEmpty()) {
                        writer.write("\nColumn_values : " + String.join(", ", query.getColumn_values()));
                    } else {
                        writer.write("\nColumn_values : Null ");
                    }
                    if (!query.getRelations().isEmpty()) {
                        writer.write("\nRelations : " + String.join(", ", query.getRelations()));
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                    writer.write("\nMSG : " + query.getMsg());
                } else if (query.getType().equals("insertnormalizedCF")) {
                    writer.write("\n\n*************************Insert normalized ColumnFamily Query*************************************");
                    writer.write("\nBase Query : " + query.getQuery());
                    writer.write("\nType : " + query.getType());
                    if (!query.getPartition_keys().isEmpty()) {
                        writer.write("\nnormalized CF Partition_keys : " + String.join(", ", query.getPartition_keys()));
                    } else {
                        writer.write("\nnormalized CF Partition_keys : Null ");
                    }
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nnormalized CF Clustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nnormalized CF Clustering_keys : Null ");
                    }
                    if (!query.getColumn_values().isEmpty()) {
                        writer.write("\nnormalized CF Column_values : " + String.join(", ", query.getColumn_values()));
                    } else {
                        writer.write("\nnormalized CF Column_values : Null ");
                    }
                    if (!query.getRelations().isEmpty()) {
                        writer.write("\nRelations : " + String.join(", ", query.getRelations()));
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                    writer.write("\nMSG : " + query.getMsg());
                } else if (query.getType().equals("updatenormalizedCF")) {
                    writer.write("\n\n*************************Update normalized ColumnFamily Query*************************************");
                    writer.write("\nBase Query : " + query.getQuery());
                    writer.write("\nType : " + query.getType());
                    if (!query.getPartition_keys().isEmpty()) {
                        writer.write("\nnormalized CF Partition_keys : " + String.join(", ", query.getPartition_keys()));
                    } else {
                        writer.write("\nnormalized CF Partition_keys : Null ");
                    }
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nnormalized CF Clustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nnormalized CF Clustering_keys : Null ");
                    }
                    if (!query.getColumn_values().isEmpty()) {
                        writer.write("\nnormalized CF Column_values : " + String.join(", ", query.getColumn_values()));
                    } else {
                        writer.write("\nnormalized CF Column_values : Null ");
                    }
                    if (!query.getRelations().isEmpty()) {
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                    writer.write("\nMSG : " + query.getMsg());
                }
            }
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public static void writeTablesToFile(List<Query> quereis, String path) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            for (Query query : quereis) {
                if (query.getType().equals("select")) {
                    writer.write("\n\n*******************Read Query*****************");
                    writer.write("\nQuery : " + query.getQuery());
                    writer.write("\nType : " + query.getType());
                    writer.write("\nweight : " + query.getWeight());
                    writer.write("\nPartition_keys : " + String.join(", ", query.getPartition_keys()));
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nClustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nClustering_keys : Null ");
                    }
                    writer.write("\nColumn_values : " + String.join(", ", query.getColumn_values()));
                    writer.write("\nEntities : " + String.join(", ", query.getEntities()));
                    if (!query.getRelations().isEmpty()) {
                        writer.write("\nRelations : " + String.join(", ", query.getRelations()));
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                    if (query.isDeleted()) {
                        writer.write("\nDeleted : " + query.isDeleted());
                        writer.write("\nMsg : " + query.getMsg());
                    }
                } else if (query.getType().equals("view")) {
                    writer.write("\n\n*******************Materialized Views Query*****************");
                    writer.write("\nBase Query : " + query.getQuery());
                    writer.write("\nType : " + query.getType());
                    writer.write("\nPartition_keys : " + String.join(", ", query.getPartition_keys()));
                    if (!query.getClustering_keys().isEmpty()) {
                        writer.write("\nClustering_keys : " + String.join(", ", query.getClustering_keys()));
                    } else {
                        writer.write("\nClustering_keys : Null ");
                    }
                    writer.write("\nColumn_values : " + String.join(", ", query.getColumn_values()));
                    if (!query.getRelations().isEmpty()) {
                        writer.write("\nRelations : " + String.join(", ", query.getRelations()));
                    } else {
                        writer.write("\nRelations : NULL");
                    }
                }

            }
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Schema.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
