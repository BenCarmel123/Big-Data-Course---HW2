package bigdatacourse.hw2.studentcode;

import java.time.Instant;
import java.util.List;
import org.json.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Task that binds JSON objects to a prepared Cassandra statement and executes
 * them. Used by worker threads to insert items or reviews.
 */
public class InsertTask implements Runnable {

    // Insert kinds: different bind order per type
    public enum Type { ITEM, USER_REVIEW, ITEM_REVIEW }

    // Data to insert (each JSONObject is one record)
    private final List<JSONObject> data;
    // Prepared statement reused across tasks
    private final PreparedStatement stmt;
    // Session used to execute statements
    private final CqlSession session;
    // Which mapping to use when binding
    private final Type type;

    public InsertTask(List<JSONObject> data, PreparedStatement stmt, CqlSession session, Type type) {
        this.data = data;
        this.stmt = stmt;
        this.session = session;
        this.type = type;
    }

    // Process each JSON object, bind fields depending on `type`, execute.
    // Exceptions are caught per-record so one bad object won't stop the rest.
    @Override
    public void run() {
        for (JSONObject obj : data) {
            try {
                switch (type) {
                    case ITEM:
                        session.execute(stmt.bind(
                            obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            parseList(obj, "categories"),
                            obj.optString("description", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optString("title", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            (float) obj.optDouble("price", 0.0),
                            obj.optString("brand", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optString("imUrl", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            parseList(obj, "alsoBought"),
                            parseList(obj, "alsoViewed")
                        ));
                        break;

                    case USER_REVIEW: {
                        long unixTime = obj.optLong("unixReviewTime", 0);
                        Instant reviewTime = Instant.ofEpochSecond(unixTime);
                        session.execute(stmt.bind(
                            obj.optString("reviewerID", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            reviewTime,
                            obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optString("reviewerName", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            parseIntList(obj, "helpful"),
                            obj.optString("reviewText", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optInt("overall", 0),
                            obj.optString("summary", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            unixTime
                        ));
                        break;
                    }

                    case ITEM_REVIEW: {
                        long unixTime = obj.optLong("unixReviewTime", 0);
                        Instant reviewTime = Instant.ofEpochSecond(unixTime);
                        session.execute(stmt.bind(
                            obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            reviewTime,
                            obj.optString("reviewerID", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optString("reviewerName", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            parseIntList(obj, "helpful"),
                            obj.optString("reviewText", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            obj.optInt("overall", 0),
                            obj.optString("summary", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                            unixTime
                        ));
                        break;
                    }

                }
            } catch (Exception e) {
                // Simple logging; keep going with next object
                System.out.println("InsertTask error: " + e.getMessage());
            }
        }
    }

    // Convert a JSON array at `key` to a List<String>. Return empty list if
    // missing or not an array.
    private static List<String> parseList(JSONObject obj, String key) {
        List<String> list = new java.util.ArrayList<>();
        if (obj.has(key) && obj.optJSONArray(key) != null) {
            for (Object o : obj.optJSONArray(key)) {
                list.add(o.toString());
            }
        }
        return list;
    }

    // Convert numeric JSON array elements at `key` to List<Integer>.
    private static List<Integer> parseIntList(JSONObject obj, String key) {
        List<Integer> list = new java.util.ArrayList<>();
        if (obj.has(key) && obj.optJSONArray(key) != null) {
            for (Object o : obj.optJSONArray(key)) {
                if (o instanceof Number) list.add(((Number) o).intValue());
            }
        }
        return list;
    }
}
