SELECT

DATE(report_interval_start_time) AS report_date,

CASE WHEN report_mean_samples_0_type = 'ENERGY_SUMMARY_MEAN_TYPE_INDOOR_TEMPERATURE_LOW' THEN report_mean_samples_0_value ELSE NULL END AS mean_low_temp,
CASE WHEN report_mean_samples_1_type = 'ENERGY_SUMMARY_MEAN_TYPE_INDOOR_TEMPERATURE_HIGH' THEN report_mean_samples_1_value ELSE NULL END AS mean_high_temp,
CASE WHEN report_mean_samples_2_type = 'ENERGY_SUMMARY_MEAN_TYPE_INDOOR_HUMIDITY_LOW' THEN report_mean_samples_2_value ELSE NULL END AS mean_low_humidity,
CASE WHEN report_mean_samples_3_type = 'ENERGY_SUMMARY_MEAN_TYPE_INDOOR_HUMIDITY_HIGH' THEN report_mean_samples_3_value ELSE NULL END AS mean_high_humidity,
CASE WHEN report_duration_samples_0_type = 'ENERGY_SUMMARY_DURATION_TYPE_HEATING' THEN report_duration_samples_0_value ELSE NULL END AS heating_duration,
CASE WHEN report_duration_samples_1_type = 'ENERGY_SUMMARY_DURATION_TYPE_LEAF' THEN report_duration_samples_1_value ELSE NULL END AS leaf_duration

FROM thermostat_events 

WHERE type = 'type.nestlabs.com/nest.trait.hvac.EnergySummaryTrait.DailyEnergySummaryEvent'

