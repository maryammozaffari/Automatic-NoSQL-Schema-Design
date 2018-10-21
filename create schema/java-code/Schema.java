
import model.Query;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import model.Models;


public class Schema {

    public static void main(String[] args) {
        ArrayList<Query> queries = new ArrayList<>();
        ArrayList<Query> cfPrim = new ArrayList<>();
        String tables = args[0];
        String supportTables = args[1];

        try (Stream<String> stream = Files.lines(Paths.get(args[2]))) {
            stream.forEach((String q) -> {
                String str = q.split("-")[1].trim();
                int index = q.indexOf("-");
                String removePart = q.substring(index, q.length());
                String weight = str.split(":")[1].trim();
                String weight_type = str.split(":")[0].trim();
                String stringQuery = q.replace(removePart, "").trim();
                Query query = new Query(stringQuery.toLowerCase());
                query.setWeight(Double.valueOf(weight.trim()));
                query.setWeight_type(weight_type);
                if (query.getQuery().startsWith("select")) {
                    query.setType("select");
                    queries.add(query);
                } else if (query.getQuery().startsWith("update")) {
                    query.setType("update");
                    queries.add(query);
                } else if (query.getQuery().startsWith("insert")) {
                    query.setType("insert");
                    queries.add(query);
                }
            });
            createReadQ(queries);
            mergeQueries(queries);
            Utils.writeTablesToFile(queries, tables);
            int size = queries.size();
            for (int i = 0; i < size; i++) {
                if (queries.get(i).getType().equals("update")) {
                    UpdateAndInsert.createUpdateReq(queries, queries.get(i), cfPrim);
                } else if (queries.get(i).getType().equals("insert")) {
                    UpdateAndInsert.createInsertReq(queries, queries.get(i), cfPrim);
                }
            }
           Utils.writeSupportTablesToFile(cfPrim, supportTables);
        } catch (Exception e) {
            e.printStackTrace();
        };
    }

    public static void createReadQ(List<Query> queries) {
        Models models = new Models();
        for (Query query : queries) {
            if (query.getType().equals("select")) {
                String select_part;
                String from_part;
                String where_part;
                String order_part = null;
                int order_index = 0;
                int select_index = query.getQuery().toLowerCase().indexOf("select");
                int from_index = query.getQuery().toLowerCase().indexOf("from");
                int where_index = query.getQuery().toLowerCase().indexOf("where");
                if (query.getQuery().contains("order")) {
                    order_index = query.getQuery().toLowerCase().indexOf("order");
                }
                select_part = query.getQuery().substring(select_index + 6, from_index).trim();
                StringBuilder stringBuilder = new StringBuilder();
                if (select_part.contains("*")) {
                    for (String str : select_part.split(",")) {
                        stringBuilder.append(Utils.getAllEntityFields(models, str.trim()));
                    }
                    select_part = stringBuilder.toString();
                }
                from_part = query.getQuery().substring(from_index + 5, where_index).trim();
                if (order_index == 0) {
                    where_part = query.getQuery().substring(where_index + 6, query.getQuery().length() - 1).trim();
                } else {
                    where_part = query.getQuery().substring(where_index + 6, order_index).trim();
                    order_part = query.getQuery().substring(order_index + 9, query.getQuery().length() - 1).trim();
                }
                // add where clause fields to partition and clustering keys
                for (String str : where_part.split("and")) {
                    if (str.trim().split(" ")[1].equals("=")) {
                        query.getPartition_keys().add(str.trim().split(" ")[0]);
                    } else {
                        query.getClustering_keys().add(str.trim().split(" ")[0]);
                    }
                }

                // process orderby clause fields to clustering keys
                if (order_part != null) {
                    for (String str : order_part.toLowerCase().split(",")) {
                        query.getClustering_keys().add(str.trim());
                    }
                }

                // process form clause fields
                for (String str : from_part.split("\\.")) {
                    if (models.getEntites().contains(str.trim())) {
                        query.getEntities().add(str.trim());
                        if (!(query.getPartition_keys().contains(str.trim() + ".id")) || (query.getClustering_keys().contains(str.trim() + ".id"))) {
                            query.getClustering_keys().add(str.trim() + ".id");
                        }
                    } else {
                        query.getRelations().add(str.trim());
                    }
                }

                // process select clause
                for (String str : select_part.split(",")) {
                    if (!((query.getPartition_keys().contains(str.trim())) || (query.getClustering_keys().contains(str.trim())))) {
                        query.getColumn_values().add(str.trim());
                    }
                }
            }
            query.mergeAllCol();
        }
    }

   

    public static void mergeQueries(ArrayList<Query> queries) {

        // First Rules
        for (int i = 0; i < queries.size(); i++) {
            for (int j = i + 1; j < queries.size(); j++) {
                if (queries.get(i).getType().equals("select") && queries.get(j).getType().equals("select")) {
                    if ((queries.get(i).getPartition_keys_string().equals(queries.get(j).getPartition_keys_string()))
                            && (queries.get(i).getClustering_keys().containsAll(queries.get(j).getClustering_keys()))
                            && (queries.get(i).getRelations().containsAll(queries.get(j).getRelations()))) {

                        if (queries.get(i).getAllCols().containsAll(queries.get(j).getColumn_values())) {
                            if (!queries.get(i).isDeleted()) {
                                queries.get(j).setDeleted(true);
                                queries.get(j).setMsg("Rule 1- Merge Read\nOrigin Query: " + queries.get(i).getQuery());
                            }
                        }
                    }
                    if ((queries.get(j).getPartition_keys_string().equals(queries.get(i).getPartition_keys_string()))
                            && (queries.get(j).getClustering_keys().containsAll(queries.get(i).getClustering_keys()))
                            && (queries.get(j).getRelations().containsAll(queries.get(i).getRelations()))) {
                        if (queries.get(j).getAllCols().containsAll(queries.get(i).getColumn_values())) {
                            if (!queries.get(j).isDeleted()) {
                                queries.get(i).setDeleted(true);
                                queries.get(i).setMsg("Rule 1- Merge Read\nOrigin Query: " + queries.get(j).getQuery());
                            }
                        }
                    }
                }
            }
        }
        // Second Rule
        List<Query> rule2views = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            for (int j = i + 1; j < queries.size(); j++) {
                if (queries.get(i).getType().equals("select") && queries.get(j).getType().equals("select") && (!queries.get(i).isDeleted()) && (!queries.get(j).isDeleted())) {
                    if ((queries.get(i).getAllCols().containsAll(queries.get(j).getAllCols()))
                            && (queries.get(i).getRelations().containsAll(queries.get(j).getRelations()))
                            && (queries.get(j).getPartition_keys().containsAll(queries.get(i).getPartition_keys()))) {
                        queries.get(j).setDeleted(true);
                        queries.get(j).setMsg("Rule 2 - Merge Read \nOrigin: " + queries.get(i).getQuery());
                        rule2views.add(UpdateAndInsert.createMaterializedView(queries.get(j), queries.get(i)));
                    }

                    if ((queries.get(j).getAllCols().containsAll(queries.get(i).getAllCols()))
                            && (queries.get(j).getRelations().containsAll(queries.get(i).getRelations()))
                            && (queries.get(i).getPartition_keys().containsAll(queries.get(j).getPartition_keys()))) {
                        queries.get(i).setDeleted(true);
                        queries.get(i).setMsg("Rule 2 - Merge Read \nOrigin: " + queries.get(j).getQuery());
                        rule2views.add(UpdateAndInsert.createMaterializedView(queries.get(i), queries.get(j)));
                    }
                }
            }
        }
        queries.addAll(rule2views);
    }
}
