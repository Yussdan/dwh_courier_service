CREATE TABLE stg.couriers(
	id SERIAL PRIMARY key,
	object_value text,
	update_ts date);
CREATE TABLE stg.deliveries(
	id SERIAL PRIMARY KEY,
	object_value TEXT,
	update_ts date);
