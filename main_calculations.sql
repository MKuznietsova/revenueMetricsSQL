with games_paid_users_payments as (
	select gp.user_id, gp.game_name, gp.payment_date, gp.revenue_amount_usd, gpu.language, gpu.has_older_device_model, gpu.age
	from project.games_payments gp
	left join project.games_paid_users gpu on gp.user_id = gpu.user_id
),
tmp as (
select
	user_id,
	game_name,
	language,
	age,
	date(date_trunc ('month', payment_date)) AS payment_month,
	--ARPPU, LT & LTV
	sum(revenue_amount_usd)/count(user_id) AS ARPPU,
	max(date(payment_date)) - min(date(payment_date)) as LT,
	sum(revenue_amount_usd) * (max(date(payment_date)) - min(date(payment_date))) as LTV,
	sum(revenue_amount_usd) AS MRR
from games_paid_users_payments gpup
group by
	1,2,3,4, 5),
--MRR, NEW MRR, New Paying Users, Paying Users per month
dates as (
select 
	*,
	--розрахунок previous payment month, якщо користувач не платив в минулому місяці, вважаємо його новим платником
	lag (payment_month) over (partition by user_id order by payment_month) as prev_payment_month,
	--рахуємо останній місяць оплати, після якого платник стає churned
	max (payment_month) over (partition by user_id) as last_payment_month,
	--рахуємо наступний місяць оплати
	lead(payment_month) over (partition by user_id order by payment_month) as next_payment_month,
	--календарні місяці відносно поточного місяця оплати
	date(payment_month - interval '1' month) as prev_cal_month,
	date(payment_month + interval '1' month) as next_cal_month,
	--розраховуєм попередню revenue відносно поточного місяця оплати
	lag(MRR) over (partition by user_id order by payment_month) as prev_month_mrr
from tmp
),
nmrr as (
select 
	*,
	case when prev_payment_month is null then mrr else 0 end as new_mrr,
	case when prev_payment_month is null then 1 else 0 end as new_paying_user,
	case when last_payment_month = payment_month then 1 else 0 end as churned_user,
	case when last_payment_month = payment_month then mrr else 0 end as churned_revenue,
	case when prev_payment_month = prev_cal_month and MRR > prev_month_mrr then MRR - prev_month_mrr else 0 end as expansion_mrr,
	case when prev_payment_month = prev_cal_month and MRR < prev_month_mrr then MRR - prev_month_mrr else 0 end as contraction_mrr
from dates
)
select
	user_id,
	age,
	language,
	game_name,
	payment_month,
	ARPPU,
	count(distinct user_id) as paying_users,
	sum (mrr) as mrr,
	sum(new_paying_user) as new_paying_users,
	sum(new_mrr) as new_mrr,
	sum (churned_user) as churned_users,
	sum (churned_revenue) as churned_revenue,
	sum (expansion_mrr) as expansion_mrr,
	sum (contraction_mrr) as contraction_mrr,
	avg (LTV) as LTV,
	avg (LT) as LT
from nmrr
group by 1,2,3,4,5,6;
