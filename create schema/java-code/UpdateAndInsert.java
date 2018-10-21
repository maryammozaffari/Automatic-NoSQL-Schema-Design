
import model.Query;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
public class UpdateAndInsert {

    public static void createUpdateReq(List<Query> queries, Query query, List<Query> cfPrim) {
        String update_entity;
        String set_clause;
        String where_clause;
        Set<String> update_entity_set = new HashSet<>();
        Set<String> set_clause_set = new HashSet<>();
        Set<String> where_partition_clause_set = new HashSet<>();
        Set<String> where_clustering_clause_set = new HashSet<>();
        int update_index = query.getQuery().indexOf("update");
        int set_index = query.getQuery().indexOf("set");
        int where_index = query.getQuery().indexOf("where");
        update_entity = query.getQuery().substring(update_index + 7, set_index).trim();
        set_clause = query.getQuery().substring(set_index + 4, where_index).trim();
        where_clause = query.getQuery().substring(where_index + 6, query.getQuery().length() - 1).trim();
        update_entity_set.addAll(Arrays.asList(update_entity.split(",")));
        for (String str : set_clause.split(",")) {
            set_clause_set.add(str.trim().split(" ")[0].trim());
        }
        for (String str : where_clause.split(",")) {
            if (str.trim().split(" ")[1].trim().equals("=")) {
                where_partition_clause_set.add(str.trim().split(" ")[0].trim());
            } else {
                where_clustering_clause_set.add(str.trim().split(" ")[0].trim());
            }
        }
        query.setPartition_keys(where_partition_clause_set);
        query.setClustering_keys(where_clustering_clause_set);
        query.setColumn_values(set_clause_set);
        query.setEntities(update_entity_set);
        query.mergeAllCol();
        getInfluencedByUpdate(query, queries);
        cfPrim.addAll(creatingSupportColumnFamily(queries, query));
    }

    public static void createInsertReq(List<Query> queries, Query query, List<Query> cfPrim) {
        String insert_entity;
        String set_clause;
        String linkTo_clause;

        Set<String> set_clause_set = new HashSet<>();
        Set<String> insert_entity_set = new HashSet<>();
        Set<String> linkTo_relations = new HashSet<>();
        Set<String> linkTo_entities = new HashSet<>();

        int insert_index = query.getQuery().indexOf("insert");
        int set_index = query.getQuery().indexOf("set");
        int linkTO_index = query.getQuery().indexOf("linkto");
        insert_entity = query.getQuery().substring(insert_index + 12, set_index).trim();
        set_clause = query.getQuery().substring(set_index + 4, linkTO_index).trim();
        insert_entity_set.add(insert_entity);
        for (String str : set_clause.split(",")) {
            set_clause_set.add(str.trim().split(" ")[0].trim());
        }
        linkTo_clause = query.getQuery().substring(linkTO_index + 7, query.getQuery().length() - 1).trim();
        for (String str : linkTo_clause.split(",")) {
            linkTo_relations.add(str.substring(0, str.indexOf(".")).trim());
            linkTo_entities.add(str.substring(str.indexOf(".") + 1, str.indexOf("(")));
        }
        query.getEntities().addAll(insert_entity_set);
        query.getLinkTo_entities().addAll(linkTo_entities);
        query.getLinkTo_relations().addAll(linkTo_relations);
        query.getColumn_values().addAll(set_clause_set);
        query.mergeAllCol();
        getInfluencedByUpdate(query, queries);
        cfPrim.addAll(creatingSupportColumnFamily(queries, query));
    }

    public static List<Query> creatingSupportColumnFamily(List<Query> queries, Query query) {
        List<Query> result = new ArrayList<>();
        Set<String> wrEntitiesAttr = new HashSet<>(Arrays.asList(Utils.getAllEntityFields(new Models(), query.getEntities_string() + ".*").replaceAll(" ", "").split(",")));
        if (query.getType().equals("update")) {
            for (Query cf : queries) {
                if (cf.isEffectFlage()) {
                    cf.setEffectFlage(false);

                    // check for creating Normalization 
                    if ((cf.getEntities().size() != 1) && (cf.getAllCols().containsAll(wrEntitiesAttr)) && (query.getWeight() > cf.getWeight())) {
                        result.add(createNormalizedCF(query, cf));
                    } else if ((!cf.getPartition_keys_string().equals(query.getPartition_keys_string())) || (!cf.getClustering_keys_string().equals(query.getClustering_keys_string()))) {
                        Query q = new Query();
                        q.setType("UpdateSupportColumnFamily");
                        q.setQuery(query.getQuery());
                        q.getPartition_keys().addAll(query.getPartition_keys());
                        q.getClustering_keys().addAll(query.getClustering_keys());
                        Set<String> tmpSet = new HashSet<>();
                        cf.getEntities().stream().forEach((s) -> {
                            tmpSet.add(s + ".id");
                        });
                        tmpSet.removeAll(q.getPartition_keys());
                        Set<String> cf_primary_keys = new HashSet<>(Utils.getUnion(cf.getPartition_keys(), cf.getClustering_keys()));
                        cf_primary_keys.removeAll(q.getPartition_keys());
                        for (String attr : cf_primary_keys) {
                            if (tmpSet.contains(attr)) {
                                q.getClustering_keys().add(attr);
                            } else {
                                q.getColumn_values().add(attr);
                            }
                        }
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("base CF: ").append(cf.getQuery()).append("\nbase Partition : ")
                                .append(cf.getPartition_keys()).append("\nbase Clustering : ")
                                .append(cf.getClustering_keys()).append("\nbase Entities : ")
                                .append(cf.getEntities());
                        q.setMsg(stringBuilder.toString());
                        result.add(q);
                    }
                }
            }
        } else if (query.getType().equals("insert")) {
            model.Models models = new Models();
            for (Query cf : queries) {
                if (cf.isEffectFlage()) {
                    cf.setEffectFlage(false);

                    // check for creating Normalization 
                    if ((cf.getEntities().size() != 1) && (cf.getAllCols().containsAll(wrEntitiesAttr)) && (query.getWeight() > cf.getWeight())) {
                        result.add(createNormalizedCF(query, cf));
                    } else if (cf.getEntities().size() != 1) {

                        Set<String> tmpRelationship = new HashSet<>(cf.getRelations());
                        tmpRelationship.retainAll(query.getLinkTo_relations());
                        if (tmpRelationship.isEmpty()) {
                            continue;
                        }
                        Set<String> E = query.getLinkTo_entities();
                        String ePrim = query.getEntities().iterator().next();
                        Set<String> pk = new HashSet<>();
                        for (String en : E) {
                            pk.add(en + ".id");
                        }
                        Set<String> A = cf.getAllCols();
                        A.removeAll(Utils.getUnion(pk, query.getColumn_values()));
                        if (!A.isEmpty()) {
                            Map<String, Integer> mapA = A.stream().collect(Collectors.toMap(x -> x, x -> 0));
                            for (String e : E) {
                                Query q = new Query();
                                q.setType("InsertSupportColumnFamily");
                                q.setQuery(query.getQuery());
                                for (String attr : mapA.keySet()) {
                                    if (mapA.get(attr) == 0) {
                                        String attr_entity = attr.substring(0, attr.indexOf("."));
                                        String e_attr_relation = models.getRelations().containsKey(e + "=" + attr_entity) ? models.getRelations().get(e + "=" + attr_entity) : models.getRelations().get(attr_entity + "=" + e);
                                        // (models.getRelations().containsValue(e + "=" + attr_entity)) || (models.getRelations().containsValue(attr_entity + "=" + e))
                                        if ((e.equals(attr_entity)) || (cf.getRelations().contains(e_attr_relation))) {
                                            q.getPartition_keys().add(e + ".id");
                                            Set<String> cf_entities = new HashSet<>();
                                            for (String en : cf.getEntities()) {
                                                cf_entities.add(en + ".id");
                                            }
                                            if (cf_entities.contains(attr)) {
                                                q.getClustering_keys().add(attr);
                                            } else {
                                                q.getColumn_values().add(attr);
                                            }
                                            // delete attr from A
                                            mapA.put(attr, 1);
                                        }
                                    }
                                }
                                if (!q.getPartition_keys().isEmpty()) {
                                    StringBuilder stringBuilder = new StringBuilder();
                                    stringBuilder
                                            .append("\nbase CF: ").append(cf.getQuery())
                                            .append("\nbase Partition : ").append(cf.getPartition_keys())
                                            .append("\nbase Clustering : ").append(cf.getClustering_keys())
                                            .append("\nbase Columns : ").append(cf.getColumn_values())
                                            .append("\nbase Entities : ").append(cf.getEntities());
                                    q.setMsg(stringBuilder.toString());
                                    result.add(q);
                                }

                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public static Query createNormalizedCF(Query wrQuery, Query cf1) {
        Query cf = new Query(cf1);
        Query cfn = new Query();
        if (wrQuery.getType().equals("insert")) {
            cfn.setType("insertnormalizedCF");
        } else {
            cfn.setType("updatenormalizedCF");
        }
        cfn.setQuery(wrQuery.getQuery());
        StringBuilder oldQueryData = new StringBuilder();
        oldQueryData.append("\nOld Partition Key: ").append(cf.getPartition_keys())
                .append("\nOld Clustering Key: ").append(cf.getClustering_keys())
                .append("\nOld Column Value: ").append(cf.getColumn_values());
        Set<String> wrEntitiesAttr = new HashSet<>(Arrays.asList(Utils.getAllEntityFields(new Models(), wrQuery.getEntities_string() + ".*").replaceAll(" ", "").split(",")));

        String wrEntityPrimaryKey = wrQuery.getEntities_string() + ".id";
        for (String attr : cf.getAllCols()) {
            if (wrEntitiesAttr.contains(attr)) {
                if (wrEntityPrimaryKey.equals(attr)) {
                    cfn.getPartition_keys().add(attr);
                } else if (cf.getColumn_values().contains(attr)) {
                    cf.getColumn_values().remove(attr);
                    cfn.getColumn_values().add(attr);
                } else if (cf.getClustering_keys().contains(attr)) {
                    cf.getClustering_keys().remove(attr);
                    cfn.getClustering_keys().add(attr);
                }
            }
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\nbase CF: ").append(cf.getQuery())
                .append(oldQueryData.toString())
                .append("\n\nnew Partition Key: ").append(cf.getPartition_keys())
                .append("\nnew Clustering Key: ").append(cf.getClustering_keys())
                .append("\nnew Columns Value: ").append(cf.getColumn_values());
        cfn.setMsg(stringBuilder.toString());
        return cfn;
    }
    
    public static Query createMaterializedView(Query deletedQuery, Query baseQuery) {

        Query query = new Query();
        query.setType("view");
        query.setQuery(baseQuery.getQuery() + "\nDeleted Query : " + deletedQuery.getQuery());
        query.setPartition_keys(Utils.getUnion(baseQuery.getPartition_keys(), deletedQuery.getPartition_keys()));
        query.setClustering_keys(Utils.getUnion(deletedQuery.getClustering_keys(), baseQuery.getClustering_keys()));
        query.setColumn_values(deletedQuery.getColumn_values());
        Set<String> tmp = new HashSet<>(Utils.getUnion(baseQuery.getPartition_keys(), baseQuery.getClustering_keys()));
        Set<String> diff = new HashSet<>(Utils.getUnion(deletedQuery.getPartition_keys(), deletedQuery.getClustering_keys()));
        diff.removeAll(tmp);
        query.setMaterializedViews(diff);
        return query;
    }

    public static void getInfluencedByUpdate(Query q, List<Query> queries) {
        List<Query> list = new ArrayList<>();
        for (Query query : queries) {
            if (query.getType().equals("select") && (!query.isDeleted())) {
                if (!Collections.disjoint(query.getAllCols(), q.getColumn_values())) {
                    query.setEffectFlage(true);
                    list.add(query);
                }
            }
        }
    }
}
