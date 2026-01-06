package bigdatacourse.hw2.studentcode;

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;

import org.json.JSONObject;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertTask implements Runnable {

    public enum Type { ITEM, USER_REVIEW, ITEM_REVIEW }

    private final String jsonLine;
    private final PreparedStatement stmt;
    private final CqlSession session;
    private final Type type;
    public static AtomicInteger userCount = new AtomicInteger(0);
    public static AtomicInteger itemCount = new AtomicInteger(0);
    public static AtomicInteger itemsCount = new AtomicInteger(0);

    public InsertTask(String jsonLine,
                      PreparedStatement stmt,
                      CqlSession session,
                      Type type) {
        this.jsonLine = jsonLine;
        this.stmt = stmt;
        this.session = session;
        this.type = type;
    }

    @Override
    public void run() {
    try {
        JSONObject obj = new JSONObject(jsonLine);

        switch (type) {

        case ITEM:
            session.execute(stmt.bind(
                obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                obj.optString("title", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                obj.optString("imUrl", HW2StudentAnswer.NOT_AVAILABLE_VALUE),
                parseList(obj, "categories"),
                obj.optString("description", HW2StudentAnswer.NOT_AVAILABLE_VALUE)
            ));
            itemsCount.incrementAndGet();
            break;

        case USER_REVIEW: {
            long unixTime = obj.optLong("unixReviewTime", 0);
            Instant reviewTime = Instant.ofEpochSecond(unixTime); 

            session.execute(stmt.bind()
                .setString(0, obj.optString("reviewerID", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setInstant(1, reviewTime)
                .setString(2, obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setString(3, obj.optString("reviewerName", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setFloat(4, (float) obj.optDouble("overall", 0.0))
                .setString(5, obj.optString("summary", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setString(6, obj.optString("reviewText", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
            );
             int count = userCount.incrementAndGet();
            if (count % 50_000 == 0) {
                System.out.println("Inserted USER_REVIEW: " + count);
            }
            break;
        }

        case ITEM_REVIEW: {
            long unixTime = obj.optLong("unixReviewTime", 0);
            Instant reviewTime = Instant.ofEpochSecond(unixTime); 

            session.execute(stmt.bind()
                .setString(0, obj.optString("asin", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setInstant(1, reviewTime)
                .setString(2, obj.optString("reviewerID", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setString(3, obj.optString("reviewerName", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setFloat(4, (float) obj.optDouble("overall", 0.0))
                .setString(5, obj.optString("summary", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
                .setString(6, obj.optString("reviewText", HW2StudentAnswer.NOT_AVAILABLE_VALUE))
            );
           int count = itemCount.incrementAndGet();
            if (count % 50_000 == 0) {
                System.out.println("Inserted ITEM_REVIEW: " + count);
            }
        }
        }

        Thread.sleep(10); 

    } catch (Exception e) {
        System.out.println("InsertTask error: " + e.getMessage());
    }
}


    private static List<String> parseList(JSONObject obj, String key) {
        List<String> list = new ArrayList<>();
        if (obj.has(key) && obj.optJSONArray(key) != null) {
            for (Object o : obj.optJSONArray(key)) {
                list.add(o.toString());
            }
        }
        return list;
    }
}
