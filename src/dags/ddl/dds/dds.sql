CREATE TABLE dds.dm_couriers(
	id SERIAL PRIMARY KEY,
	courier_id TEXT NOT null,
	courier_name VARCHAR(256));

CREATE TABLE dds.dm_deliveries(
	id SERIAL PRIMARY KEY,
	order_id INT NOT null REFERENCES dds.dm_orders(id),
	timestamp_id INT NOT NULL REFERENCES dds.dm_timestamps(id),
	delivery_id TEXT NOT NULL,
	courier_id INT NOT NULL REFERENCES dds.dm_couriers(id),
	rate INT NOT NULL,
	tip_sum NUMERIC(14,2) NOT NULL 
);

ALTER TABLE dds.dm_orders 
	ADD COLUMN deliveries_id int REFERENCES dds.dm_deliveries(id);

alter table dds.fct_product_sales 
	add column deliveries_id int REFERENCES dds.dm_deliveries(id), 
	add column rate int, 
	add column tip_sum numeric(14,2), 
	add column courier_id int REFERENCES dds.dm_couriers(id);


