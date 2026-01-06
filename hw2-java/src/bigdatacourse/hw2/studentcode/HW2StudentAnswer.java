package bigdatacourse.hw2.studentcode;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{

	public static final String		NOT_AVAILABLE_VALUE 	=		"na";

	// CREATE TABLE for Items
	public static final String 		CREATE_ITEMS_TABLE = 
    "CREATE TABLE IF NOT EXISTS items (" +
    "asin text PRIMARY KEY, " +
    "title text, " +
    "imUrl text, " +
    "categories list<text>, " +
    "description text " +
    ");";

	// CREATE TABLE for User Reviews
	public static final String 		CREATE_USER_REVIEWS_TABLE = 
    "CREATE TABLE IF NOT EXISTS user_reviews (" +
    "time timestamp, " +
    "asin text, " +
	"reviewerID text, " +
    "reviewerName text, " +
	"rating float, " +
	"summary text, " +
    "reviewText text, " +
    "PRIMARY KEY (reviewerID, time, asin)" +
    ") WITH CLUSTERING ORDER BY (time DESC, asin ASC);";

	// CREATE TABLE for Item Reviews
	public static final String 		CREATE_ITEM_REVIEWS_TABLE = 
    "CREATE TABLE IF NOT EXISTS item_reviews (" +
    "time timestamp, " +
    "asin text, " +
	"reviewerID text, " +
    "reviewerName text, " +
	"rating float, " +
	"summary text, " +
    "reviewText text, " +
    "PRIMARY KEY (asin, time, reviewerID)" +
    ") WITH CLUSTERING ORDER BY (time DESC, reviewerID ASC);";

	public static final String 		QUERY_ITEM = "SELECT * FROM items WHERE asin = ?;";
	public static final String 		QUERY_USER_REVIEWS = "SELECT * FROM user_reviews WHERE reviewerID = ?;";
	public static final String 		QUERY_ITEM_REVIEWS = "SELECT * FROM item_reviews WHERE asin = ?;";

	public static final String 		INSERT_ITEM =
	"INSERT INTO items (asin, title, imUrl, categories, description) " +
	"VALUES (?, ?, ?, ?, ?);";

	public static final String 		INSERT_USER_REVIEW = 
    "INSERT INTO user_reviews (reviewerID, time, asin, reviewerName, rating, summary, reviewText) " +
	"VALUES (?, ?, ?, ?, ?, ?, ?);";

	public static final String 		INSERT_ITEM_REVIEW = 
    "INSERT INTO item_reviews (asin, time, reviewerID, reviewerName, rating, summary, reviewText) " +
	"VALUES (?, ?, ?, ?, ?, ?, ?);";

	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	private PreparedStatement insertItemStmt;
	private PreparedStatement insertUserReviewStmt;
	private PreparedStatement insertItemReviewStmt;

	private PreparedStatement selectItemStmt;
	private PreparedStatement selectUserReviewsStmt;
	private PreparedStatement selectItemReviewsStmt;
	
	@Override
	/**
	 * Open a Cassandra session using the provided secure bundle and credentials.
	 */
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}

	@Override
	/**
	 * Close the Cassandra session if it is open.
	 */
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	@Override
	/**
	 * Create required keyspace tables (items, user_reviews, item_reviews).
	 */
	public void createTables() {
		System.out.println("Creating tables...");

		try {
			// Execute the CQL queries to create tables
			session.execute(CREATE_ITEMS_TABLE);
			session.execute(CREATE_USER_REVIEWS_TABLE);
			session.execute(CREATE_ITEM_REVIEWS_TABLE);

			System.out.println("Tables created successfully.");
			} 
		catch (Exception e) {
				System.out.println("Error creating tables: " + e.getMessage());
				e.printStackTrace();
			}
	}

	@Override
	/**
	 * Prepare commonly used CQL statements for later execution.
	 */
	public void initialize() {
		System.out.println("Initializing prepared statements...");
		try {

			// PreparedStatement for inserting into items table
			insertItemStmt = session.prepare(INSERT_ITEM);

			// PreparedStatement for inserting into user_reviews table
			insertUserReviewStmt = session.prepare(INSERT_USER_REVIEW);

			// PreparedStatement for inserting into item_reviews table
			insertItemReviewStmt = session.prepare(INSERT_ITEM_REVIEW);

			// PreparedStatement for selecting an item
			selectItemStmt = session.prepare(QUERY_ITEM);

			// PreparedStatement for selecting user reviews
			selectUserReviewsStmt = session.prepare(QUERY_USER_REVIEWS);

			// PreparedStatement for selecting item reviews
			selectItemReviewsStmt = session.prepare(QUERY_ITEM_REVIEWS);


        System.out.println("Prepared statements initialized successfully.");

		} catch (Exception e) {
			System.out.println("Error initializing prepared statements: " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		System.out.println("Loading items from file: " + pathItemsFile);

		ExecutorService pool = Executors.newFixedThreadPool(250);

		try (Scanner scanner = new Scanner(new File(pathItemsFile))) {
			while (scanner.hasNextLine()) {
				String item = scanner.nextLine();

				pool.submit(new InsertTask(
					item,
					insertItemStmt,
					session,
					InsertTask.Type.ITEM
				));
			}
		}

		pool.shutdown();
		pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		System.out.println("count - " + InsertTask.itemsCount);
		System.out.println("Finished loading items.");
	}


	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		System.out.println("Loading reviews from file: " + pathReviewsFile);

		ExecutorService pool = Executors.newFixedThreadPool(200);

		try (Scanner scanner = new Scanner(new File(pathReviewsFile))) {
			while (scanner.hasNextLine()) {
				String review = scanner.nextLine();
				pool.submit(new InsertTask(review, insertUserReviewStmt, session,
					InsertTask.Type.USER_REVIEW
				));
				pool.submit(new InsertTask(
					review, insertItemReviewStmt, session,
					InsertTask.Type.ITEM_REVIEW
				));
			}
		}

		pool.shutdown();
		pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		System.out.println("user count - " + InsertTask.userCount.get());
		System.out.println("item count - " + InsertTask.itemCount.get());
		System.out.println("Finished loading reviews.");
	}


	@Override
	/**
	 * Fetch an item by ASIN and return a formatted string representation.
	 */
	public String item(String asin) {
		var row = session.execute(selectItemStmt.bind(asin)).one();
		if (row == null) return "not exists";

		Set<String> categories = new TreeSet<>(row.getList("categories", String.class));
		return formatItem(
			row.getString("asin"),
			row.getString("title"),
			row.getString("imUrl"),
			categories,
			row.getString("description")
		);
	}
	
	
	@Override
	/**
	 * Fetch reviews for a reviewer and return them as formatted strings.
	 */
	public Iterable<String> userReviews(String reviewerID) {
		var result = session.execute(selectUserReviewsStmt.bind(reviewerID));
		ArrayList<String> reviews = new ArrayList<>();

		for (var row : result) {
			Instant time = row.getInstant("time");
			reviews.add(formatReview(
				time,
				row.getString("asin"),
				row.getString("reviewerID"),
				row.getString("reviewerName"),
				(int) row.getFloat("rating"),
				row.getString("summary"),
				row.getString("reviewText")
			));
		}

		return reviews;
	}

	@Override
	/**
	 * Fetch reviews for an item (asin) and return them as formatted strings.
	 */
	public Iterable<String> itemReviews(String asin) {
		var result = session.execute(selectItemReviewsStmt.bind(asin));
		ArrayList<String> reviews = new ArrayList<>();

		for (var row : result) {
			Instant time = row.getInstant("time");
			reviews.add(formatReview(
				time,
				row.getString("asin"),
				row.getString("reviewerID"),
				row.getString("reviewerName"),
				(int) row.getFloat("rating"),
				row.getString("summary"),
				row.getString("reviewText")
			));
		}

		return reviews;
	}
	
	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

}
