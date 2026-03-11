CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device VARCHAR(50),
    actions TEXT[],
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.event_logs (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    details JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(20),
    issue_type VARCHAR(50),
    messages JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.user_recommendations (
    user_id VARCHAR(50) PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.moderation_queue (
    review_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    review_text TEXT,
    rating INT,
    moderation_status VARCHAR(20),
    flags TEXT[],
    submitted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SCHEMA IF NOT EXISTS mart;

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON raw_data.user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_start_time ON raw_data.user_sessions(start_time);
CREATE INDEX IF NOT EXISTS idx_event_logs_timestamp ON raw_data.event_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_support_tickets_user_id ON raw_data.support_tickets(user_id);
CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON raw_data.support_tickets(status);
CREATE INDEX IF NOT EXISTS idx_moderation_queue_status ON raw_data.moderation_queue(moderation_status);

CREATE TABLE IF NOT EXISTS mart.user_activity (
    date DATE,
    user_id VARCHAR(50),
    total_sessions INT,
    avg_session_duration_minutes NUMERIC(10,2),
    total_pages_visited INT,
    unique_pages_visited INT,
    total_actions INT,
    most_frequent_action VARCHAR(50),
    devices_used TEXT[],
    last_activity TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, user_id)
);

CREATE TABLE IF NOT EXISTS mart.support_efficiency (
    date DATE,
    issue_type VARCHAR(50),
    total_tickets INT,
    open_tickets INT,
    closed_tickets INT,
    avg_resolution_time_hours NUMERIC(10,2),
    max_resolution_time_hours NUMERIC(10,2),
    min_resolution_time_hours NUMERIC(10,2),
    total_messages INT,
    unique_users INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, issue_type)
);

CREATE TABLE IF NOT EXISTS mart.moderation_stats (
    date DATE,
    moderation_status VARCHAR(20),
    total_reviews INT,
    avg_rating NUMERIC(3,2),
    total_flags INT,
    unique_products INT,
    unique_users INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, moderation_status)
);

CREATE TABLE IF NOT EXISTS mart.product_popularity (
    date DATE,
    product_id VARCHAR(50),
    total_views INT,
    total_reviews INT,
    avg_rating NUMERIC(3,2),
    times_recommended INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, product_id)
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_user_activity_updated_at 
    BEFORE UPDATE ON mart.user_activity 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_support_efficiency_updated_at 
    BEFORE UPDATE ON mart.support_efficiency 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_moderation_stats_updated_at 
    BEFORE UPDATE ON mart.moderation_stats 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_product_popularity_updated_at 
    BEFORE UPDATE ON mart.product_popularity 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();