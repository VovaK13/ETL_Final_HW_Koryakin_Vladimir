db = db.getSiblingDB('admin');

var adminExists = db.getUser('admin') !== null;

if (!adminExists) {
    db.createUser({
        user: "admin",
        pwd: "admin123",
        roles: [
            { role: "root", db: "admin" },
            { role: "userAdminAnyDatabase", db: "admin" },
            { role: "readWriteAnyDatabase", db: "admin" }
        ]
    });
    print("Пользователь admin создан");
} else {
    print("Пользователь admin уже существует, пропускаем создание");
}

db = db.getSiblingDB('analytics_source');

var analyticsUserExists = db.getUser('analytics_user') !== null;
if (!analyticsUserExists) {
    db.createUser({
        user: "analytics_user",
        pwd: "analytics123",
        roles: [
            { role: "readWrite", db: "analytics_source" },
            { role: "dbAdmin", db: "analytics_source" }
        ]
    });
    print("Пользователь analytics_user создан");
}

db.createCollection("UserSessions");
db.UserSessions.insertMany([
    {
        session_id: "sess_001",
        user_id: "user_123",
        start_time: "2024-01-10T09:00:00Z",
        end_time: "2024-01-10T09:30:00Z",
        pages_visited: ["/home", "/products", "/products/42", "/cart"],
        device: "mobile",
        actions: ["login", "view_product", "add_to_cart", "logout"]
    },
    {
        session_id: "sess_002",
        user_id: "user_456",
        start_time: "2024-01-10T10:15:00Z",
        end_time: "2024-01-10T10:45:00Z",
        pages_visited: ["/home", "/about", "/contact"],
        device: "desktop",
        actions: ["login", "view_page", "logout"]
    }
]);

db.createCollection("EventLogs");
db.EventLogs.insertMany([
    {
        event_id: "evt_1001",
        timestamp: "2024-01-10T09:05:20Z",
        event_type: "click",
        details: { page: "/products/42" }
    },
    {
        event_id: "evt_1002",
        timestamp: "2024-01-10T09:06:15Z",
        event_type: "scroll",
        details: { page: "/products/42" }
    }
]);

db.createCollection("SupportTickets");
db.SupportTickets.insertMany([
    {
        ticket_id: "ticket_789",
        user_id: "user_123",
        status: "open",
        issue_type: "payment",
        messages: [
            {
                sender: "user",
                message: "Не могу оплатить заказ.",
                timestamp: "2024-01-09T12:00:00Z"
            }
        ],
        created_at: "2024-01-09T11:55:00Z",
        updated_at: "2024-01-09T13:00:00Z"
    }
]);

db.createCollection("UserRecommendations");
db.UserRecommendations.insertMany([
    {
        user_id: "user_123",
        recommended_products: ["prod_101", "prod_205", "prod_333"],
        last_updated: "2024-01-10T08:00:00Z"
    }
]);

db.createCollection("ModerationQueue");
db.ModerationQueue.insertMany([
    {
        review_id: "rev_555",
        user_id: "user_123",
        product_id: "prod_101",
        review_text: "Отличный товар!",
        rating: 5,
        moderation_status: "pending",
        flags: ["contains_images"],
        submitted_at: "2024-01-08T10:20:00Z"
    }
]);

print("=== MongoDB INIT COMPLETED ===");
print("Database: analytics_source");
print("Collections created and data inserted:");
print("- UserSessions: " + db.UserSessions.count() + " documents");
print("- EventLogs: " + db.EventLogs.count() + " documents");
print("- SupportTickets: " + db.SupportTickets.count() + " documents");
print("- UserRecommendations: " + db.UserRecommendations.count() + " documents");
print("- ModerationQueue: " + db.ModerationQueue.count() + " documents");