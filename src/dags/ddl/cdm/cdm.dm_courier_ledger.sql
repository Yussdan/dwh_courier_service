CREATE TABLE cdm.dm_courier_ledger(
    id serial primary key, 
    courier_id int not null,
    courier_name varchar not null,
    settlement_year int constraint dm_courier_ledger_settlement_year_check check(settlement_year>2020 and settlement_year<2500)  not null,
    settlement_month int constraint courier_ledger_settlement_month_check check(settlement_month>0 and settlement_month<13) not null,
    orders_count int constraint dm_courier_ledger_orders_count_check check(orders_count>-1) not null,
    orders_total_sum numeric(14,2) constraint dm_courier_orders_total_sum_ledger_check check(orders_total_sum>-1) not null,
    rate_avg numeric(2,1) constraint dm_courier_ledger_rate_avg_check check(rate_avg>0 and rate_avg<=5) not null,
    order_processing_fee numeric(14,2) constraint dm_courier_lorder_processing_fee_edger_check check(order_processing_fee>-1) not null,
    courier_order_sum numeric(14,2) constraint dm_courier_ledger_courier_order_sum_check check(courier_order_sum>-1) not null,
    courier_tips_sum numeric(14,2) constraint dm_courier_ledger_courier_tips_sum_check check(courier_tips_sum>-1) not null,
    courier_reward_sum numeric(14,2) constraint dm_courier_ledger_courier_reward_sum_check check(courier_reward_sum>-1) not null
);
