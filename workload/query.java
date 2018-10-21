package model;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class Query {

    String query;
    String type;
    double weight;
    String weight_type;
    Set<String> partition_keys;
    Set<String> clustering_keys;
    String partition_keys_string;
    String clustering_keys_string;
    Set<String> column_values;
    Set<String> materializedViews;
    Set<String> linkTo_entities;
    Set<String> linkTo_relations;
    boolean effectFlage;
    Set<String> entities;
    Set<String> relations;
    Set<String> allCols;
    String entities_string;
    boolean deleted;
    String msg;

    public Query() {
        this.effectFlage = false;
        this.deleted = false;
        this.partition_keys = new HashSet<>();
        this.clustering_keys = new HashSet<>();
        this.column_values = new HashSet<>();
        this.materializedViews = new HashSet<>();
        this.entities = new HashSet<>();
        this.relations = new HashSet<>();
        this.allCols = new HashSet<>();
        this.linkTo_entities = new HashSet<>();
        this.linkTo_relations = new HashSet<>();
    }

    public Query(Query query) {
        this.query = query.getQuery();
        this.deleted = query.isDeleted();
        this.effectFlage = query.isEffectFlage();
        this.partition_keys = new HashSet<>();
        this.partition_keys.addAll(query.getPartition_keys());
        this.clustering_keys = new HashSet<>();
        this.clustering_keys.addAll(query.getClustering_keys());
        this.column_values = new HashSet<>();
        this.column_values.addAll(query.getColumn_values());
        this.materializedViews = new HashSet<>();
        this.entities = new HashSet<>();
        this.entities.addAll(query.getEntities());
        this.relations = new HashSet<>();
        this.relations.addAll(query.getRelations());
        this.allCols = new HashSet<>();
        this.linkTo_entities = new HashSet<>();
        this.linkTo_relations = new HashSet<>();
    }
    public Query(String query) {
        this.query = query;
        this.deleted = false;
        this.effectFlage = false;
        this.partition_keys = new HashSet<>();
        this.clustering_keys = new HashSet<>();
        this.column_values = new HashSet<>();
        this.materializedViews = new HashSet<>();
        this.entities = new HashSet<>();
        this.relations = new HashSet<>();
        this.allCols = new HashSet<>();
        this.linkTo_entities = new HashSet<>();
        this.linkTo_relations = new HashSet<>();
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Set<String> getPartition_keys() {
        return partition_keys;
    }

    public void setPartition_keys(Set<String> partition_keys) {
        this.partition_keys = partition_keys;
    }

    public Set<String> getClustering_keys() {
        return clustering_keys;
    }

    public void setClustering_keys(Set<String> clustering_keys) {
        this.clustering_keys = clustering_keys;
    }

    public Set<String> getColumn_values() {
        return column_values;
    }

    public void setColumn_values(Set<String> column_values) {
        this.column_values = column_values;
    }

    public Set<String> getEntities() {
        return entities;
    }

    public void setEntities(Set<String> entities) {
        this.entities = entities;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPartition_keys_string() {
        ArrayList<String> partition_list = new ArrayList<>(getPartition_keys());
        Collections.sort(partition_list, String.CASE_INSENSITIVE_ORDER);
        setPartition_keys_string(String.join(",", partition_list));
        return partition_keys_string;
    }

    private void setPartition_keys_string(String partition_keys_string) {
        this.partition_keys_string = partition_keys_string;
    }

    public String getClustering_keys_string() {
        ArrayList<String> cluster_list = new ArrayList<>(getClustering_keys());
        Collections.sort(cluster_list, String.CASE_INSENSITIVE_ORDER);
        setClustering_keys_string(String.join(",", cluster_list));
        return clustering_keys_string;
    }

    private void setClustering_keys_string(String clustering_keys_string) {
        this.clustering_keys_string = clustering_keys_string;
    }

    public String getEntities_string() {
        ArrayList<String> entity_list = new ArrayList<>(getEntities());
        Collections.sort(entity_list, String.CASE_INSENSITIVE_ORDER);
        setEntities_string(String.join(",", entity_list));
        return entities_string;
    }

    private void setEntities_string(String entities_string) {
        this.entities_string = entities_string;
    }

    public Set<String> getRelations() {
        return relations;
    }

    public void setRelations(Set<String> relations) {
        this.relations = relations;
    }

    public Set<String> getAllCols() {
        mergeAllCol();
        return allCols;
    }

    public void setAllCols(Set<String> allCols) {
        this.allCols = allCols;
    }

    public Set<String> getMaterializedViews() {
        return materializedViews;
    }

    public void setMaterializedViews(Set<String> materializedViews) {
        this.materializedViews = materializedViews;
    }

    public Set<String> getLinkTo_entities() {
        return linkTo_entities;
    }

    public void setLinkTo_entities(Set<String> linkTo_entities) {
        this.linkTo_entities = linkTo_entities;
    }

    public Set<String> getLinkTo_relations() {
        return linkTo_relations;
    }

    public void setLinkTo_relations(Set<String> linkTo_relations) {
        this.linkTo_relations = linkTo_relations;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getWeight_type() {
        return weight_type;
    }

    public void setWeight_type(String weight_type) {
        this.weight_type = weight_type;
    }

    public boolean isEffectFlage() {
        return effectFlage;
    }

    public void setEffectFlage(boolean effectFlage) {
        this.effectFlage = effectFlage;
    }

    public void mergeAllCol() {
        this.allCols.clear();
        this.allCols.addAll(getPartition_keys());
        this.allCols.addAll(getClustering_keys());
        this.allCols.addAll(getColumn_values());
    }

}
